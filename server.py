"""
Time-aligned 15s candle stream for EURCHF_otc. No seed.

Subscribes with subscribe_symbol_timed(asset, timedelta(seconds=15))
and appends each closed candle as it arrives.

Time-aligned candles close on 15-second boundaries of the day:
:00, :15, :30, :45. The stream yields CLOSED candles only.

The chart starts empty and fills in — one new bar every 15 seconds.
"""

import asyncio
import datetime
import os
import threading
import time

ASSET = os.getenv("ASSET", "EURCHF_otc")
TIMEFRAME = int(os.getenv("TIMEFRAME", "15"))
MAX_CANDLES = int(os.getenv("MAX_CANDLES", "150"))
SSID = os.getenv("POCKET_OPTION_SSID")

_state = {
    "candles": [],
    "connected": False,
    "error": None,
    "ticks": 0,
    "last_tick_ts": 0.0,
    "started_at": time.time(),
    "gaps_remaining": 0,
}
_lock = threading.Lock()

_loop = None
_task = None
_stop = threading.Event()


def _normalize(c):
    """
    Coerce a library candle into the shape Lightweight Charts wants.

    The Rust Candle struct serializes with 'timestamp' (seconds), but
    some paths emit 'time'. Accept either. Normalize ms -> seconds.
    """
    ts = c.get("time")
    if ts is None:
        ts = c.get("timestamp")
    if ts is None:
        raise KeyError("candle has neither 'time' nor 'timestamp'")

    ts = int(float(ts))
    if ts > 10_000_000_000:
        ts //= 1000

    return {
        "time": ts,
        "open": float(c["open"]),
        "high": float(c["high"]),
        "low": float(c["low"]),
        "close": float(c["close"]),
    }


def _count_gaps(candles):
    if len(candles) < 2:
        return 0
    gaps = 0
    for i in range(1, len(candles)):
        delta = candles[i]["time"] - candles[i - 1]["time"]
        if delta > TIMEFRAME:
            gaps += delta // TIMEFRAME - 1
    return gaps


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
            delta = datetime.timedelta(seconds=TIMEFRAME)
            stream = await client.subscribe_symbol_timed(ASSET, delta)

            with _lock:
                _state["connected"] = client.is_connected()
                _state["error"] = None

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
                    last = (
                        _state["candles"][-1]
                        if _state["candles"]
                        else None
                    )
                    if last and new_candle["time"] <= last["time"]:
                        # Duplicate or stale; drop.
                        continue

                    _state["candles"] = (
                        _state["candles"] + [new_candle]
                    )[-MAX_CANDLES:]

                    _state["ticks"] += 1
                    _state["last_tick_ts"] = time.time()
                    _state["connected"] = client.is_connected()
                    _state["gaps_remaining"] = _count_gaps(
                        _state["candles"]
                    )

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
            "error": _state["error"],
            "candles": list(_state["candles"]),
            "forming": None,
            "ticks": _state["ticks"],
            "gaps_remaining": _state["gaps_remaining"],
            "last_tick_age_s": round(now - last, 2) if last else None,
            "uptime_s": round(now - _state["started_at"], 1),
        }
