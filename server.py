"""
Time-aligned candle stream for EURCHF_otc.

Seeds with compile_candles (15s bars don't exist natively on the server),
then streams with subscribe_symbol_timed, which is the library's
time-aligned subscription path.

Time-aligned candles close on 15-second boundaries of the day:
:00, :15, :30, :45. The stream yields CLOSED candles only.
"""

import asyncio
import datetime
import os
import threading
import time

ASSET = os.getenv("ASSET", "EURCHF_otc")
TIMEFRAME = int(os.getenv("TIMEFRAME", "15"))
SEED_CANDLES = int(os.getenv("SEED_CANDLES", "150"))
SEED_TIMEOUT_S = float(os.getenv("SEED_TIMEOUT_S", "30"))
SSID = os.getenv("POCKET_OPTION_SSID")

_state = {
    "candles": [],
    "connected": False,
    "error": None,
    "seeded": False,
    "ticks": 0,
    "last_tick_ts": 0.0,
    "started_at": time.time(),
    "gaps_patched": 0,
    "gaps_remaining": 0,
    "seed_source": None,
}
_lock = threading.Lock()

_loop = None
_task = None
_stop = threading.Event()


def _normalize(c):
    """
    Coerce a library candle into the shape Lightweight Charts wants.

    The Rust Candle struct serializes with 'timestamp' (seconds), but
    some paths in the wrapper emit 'time'. Accept either.
    """
    ts = c.get("time")
    if ts is None:
        ts = c.get("timestamp")
    if ts is None:
        raise KeyError("candle has neither 'time' nor 'timestamp'")

    ts = int(float(ts))
    # Some sources emit milliseconds; normalize to seconds.
    if ts > 10_000_000_000:
        ts //= 1000

    return {
        "time": ts,
        "open": float(c["open"]),
        "high": float(c["high"]),
        "low": float(c["low"]),
        "close": float(c["close"]),
    }


def _dedupe_sorted(candles):
    seen = set()
    out = []
    for c in sorted(candles, key=lambda x: x["time"]):
        if c["time"] in seen:
            continue
        seen.add(c["time"])
        out.append(c)
    return out


def _count_gaps(candles):
    if len(candles) < 2:
        return 0
    gaps = 0
    for i in range(1, len(candles)):
        delta = candles[i]["time"] - candles[i - 1]["time"]
        if delta > TIMEFRAME:
            gaps += delta // TIMEFRAME - 1
    return gaps


async def _compile_window(client, lookback_seconds):
    try:
        raw = await asyncio.wait_for(
            client.compile_candles(ASSET, TIMEFRAME, lookback_seconds),
            timeout=SEED_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        raise RuntimeError(
            f"compile_candles timed out after {SEED_TIMEOUT_S}s "
            f"(lookback={lookback_seconds}s)"
        )

    if not raw:
        return []

    return _dedupe_sorted([_normalize(c) for c in raw])


async def _seed(client):
    lookback = SEED_CANDLES * TIMEFRAME
    candles = await _compile_window(client, lookback)

    if not candles:
        raise RuntimeError(
            f"seed returned no candles "
            f"(lookback={lookback}s, timeframe={TIMEFRAME}s)"
        )

    return candles[-SEED_CANDLES:], "compile_candles"


async def _fetch_gap(client, from_time, to_time):
    span = to_time - from_time
    if span <= 0:
        return []

    now = int(time.time())
    lookback = (now - from_time) + TIMEFRAME

    try:
        raw = await asyncio.wait_for(
            client.compile_candles(ASSET, TIMEFRAME, lookback),
            timeout=SEED_TIMEOUT_S,
        )
    except Exception as e:
        with _lock:
            _state["error"] = f"gap patch failed: {type(e).__name__}: {e}"
        return []

    if not raw:
        return []

    parsed = [_normalize(c) for c in raw]
    return [c for c in parsed if from_time < c["time"] < to_time]


async def _stream():
    from BinaryOptionsToolsV2 import PocketOptionAsync
    from BinaryOptionsToolsV2.config import Config

    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID is not set")

    config = Config(
        max_allowed_loops=3,
        reconnect_time=15,
        connection_initialization_timeout_secs=30,
        terminal_logging=False,
    )

    client = PocketOptionAsync(SSID, config=config)
    await client.wait_for_assets(timeout=60.0)

    with _lock:
        _state["connected"] = client.is_connected()
        _state["error"] = None

    retry_delay = 1.0

    while not _stop.is_set():
        try:
            seeded, source = await _seed(client)
            with _lock:
                _state["candles"] = seeded
                _state["seeded"] = True
                _state["seed_source"] = source
                _state["gaps_remaining"] = _count_gaps(seeded)

            if not seeded:
                raise RuntimeError("seed produced zero candles")

            delta = datetime.timedelta(seconds=TIMEFRAME)
            stream = await client.subscribe_symbol_timed(ASSET, delta)

            async for candle in stream:
                if _stop.is_set():
                    break

                try:
                    new_candle = _normalize(candle)
                except Exception as e:
                    print(
                        f"[stream skip] {type(e).__name__}: {e} "
                        f"— candle was {candle!r}",
                        flush=True,
                    )
                    continue

                with _lock:
                    last = _state["candles"][-1] if _state["candles"] else None
                    need_patch = (
                        last is not None
                        and new_candle["time"] - last["time"] > TIMEFRAME
                    )
                    gap_from = last["time"] if need_patch else None
                    gap_to = new_candle["time"]

                if need_patch:
                    patch = await _fetch_gap(client, gap_from, gap_to)
                    with _lock:
                        merged = _dedupe_sorted(
                            _state["candles"] + patch + [new_candle]
                        )
                        _state["candles"] = merged[-SEED_CANDLES:]
                        if patch:
                            _state["gaps_patched"] += 1
                        _state["gaps_remaining"] = _count_gaps(
                            _state["candles"]
                        )
                else:
                    with _lock:
                        last = (
                            _state["candles"][-1]
                            if _state["candles"]
                            else None
                        )
                        if not last or new_candle["time"] > last["time"]:
                            _state["candles"] = (
                                _state["candles"] + [new_candle]
                            )[-SEED_CANDLES:]
                            _state["gaps_remaining"] = _count_gaps(
                                _state["candles"]
                            )

                with _lock:
                    _state["connected"] = client.is_connected()
                    _state["ticks"] += 1
                    _state["last_tick_ts"] = time.time()
                    _state["error"] = None

            retry_delay = 1.0
            await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            with _lock:
                _state["error"] = f"{type(e).__name__}: {e}"
                _state["connected"] = False
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30.0)

    try:
        await client.shutdown()
    except Exception:
        pass


def _run_loop():
    global _loop, _task
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _task = _loop.create_task(_stream())
    try:
        _loop.run_until_complete(_task)
    except Exception as e:
        with _lock:
            _state["error"] = f"loop: {type(e).__name__}: {e}"
    finally:
        _loop.close()


_started = False
_start_lock = threading.Lock()


def start():
    global _started
    with _start_lock:
        if _started:
            return
        _started = True
        t = threading.Thread(target=_run_loop, daemon=True, name="stream")
        t.start()


def snapshot():
    with _lock:
        now = time.time()
        last = _state["last_tick_ts"]
        return {
            "asset": ASSET,
            "timeframe": TIMEFRAME,
            "connected": _state["connected"],
            "seeded": _state["seeded"],
            "seed_source": _state["seed_source"],
            "error": _state["error"],
            "candles": list(_state["candles"]),
            "forming": None,
            "ticks": _state["ticks"],
            "gaps_patched": _state["gaps_patched"],
            "gaps_remaining": _state["gaps_remaining"],
            "last_tick_age_s": round(now - last, 2) if last else None,
            "uptime_s": round(now - _state["started_at"], 1),
        }
