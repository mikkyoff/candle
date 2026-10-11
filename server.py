"""
Single-asset stream worker.

Seeds with get_candles_advanced() directly, with an explicit timeout we
control, so a slow server surfaces as an error rather than a silent
empty seed. Then subscribes for live ticks and closes the seam.

The library's get_candles_live() wraps three history fetches in 3-second
timeouts and swallows failures with `except Exception: []`. That makes
seeding unreliable for anything above ~50 candles. We don't use it.
"""

import asyncio
import os
import threading
import time

ASSET = os.getenv("ASSET", "EURJPY_otc")
TIMEFRAME = int(os.getenv("TIMEFRAME", "60"))
SEED_CANDLES = int(os.getenv("SEED_CANDLES", "150"))
SEED_TIMEOUT_S = float(os.getenv("SEED_TIMEOUT_S", "30"))
SSID = os.getenv("POCKET_OPTION_SSID")

_state = {
    "candles": [],
    "forming": None,
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
    return {
        "time": int(c["time"]),
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


async def _seed(client):
    """
    Fetch SEED_CANDLES closed candles with our own timeout and report
    which source actually returned data. The library's built-in seeding
    hides this.

    get_candles_advanced(asset, period, offset, time) anchors at `time`
    and returns [time - offset, time]. We anchor at server-now and ask
    for exactly the window we need.
    """
    server_time = await client.get_server_time()
    offset_secs = SEED_CANDLES * TIMEFRAME

    try:
        raw = await asyncio.wait_for(
            client.get_candles_advanced(
                ASSET, TIMEFRAME, offset_secs, server_time
            ),
            timeout=SEED_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        raise RuntimeError(
            f"seed timed out after {SEED_TIMEOUT_S}s "
            f"({SEED_CANDLES} candles @ {TIMEFRAME}s)"
        )

    if not raw:
        # Fall back to the non-advanced endpoint, which anchors at now
        # and uses `offset` as a count of periods rather than seconds.
        raw = await asyncio.wait_for(
            client.get_candles(ASSET, TIMEFRAME, SEED_CANDLES),
            timeout=SEED_TIMEOUT_S,
        )
        source = "get_candles"
    else:
        source = "get_candles_advanced"

    if not raw:
        raise RuntimeError("seed returned no candles from any endpoint")

    parsed = _dedupe_sorted([_normalize(c) for c in raw])
    return parsed[-SEED_CANDLES:], source


async def _fetch_gap(client, from_time, to_time):
    """Pull candles strictly between two known-good edges."""
    span = to_time - from_time - TIMEFRAME
    if span <= 0:
        return []

    offset_secs = span + TIMEFRAME
    anchor = to_time

    try:
        raw = await asyncio.wait_for(
            client.get_candles_advanced(
                ASSET, TIMEFRAME, offset_secs, anchor
            ),
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

    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID is not set")

    client = PocketOptionAsync(SSID)
    await client.wait_for_assets(timeout=60.0)

    with _lock:
        _state["connected"] = client.is_connected()
        _state["error"] = None

    retry_delay = 1.0

    while not _stop.is_set():
        try:
            # --- Seed with our own timeout so failure is visible ---
            seeded, source = await _seed(client)
            with _lock:
                _state["candles"] = seeded
                _state["seeded"] = True
                _state["seed_source"] = source
                _state["gaps_remaining"] = _count_gaps(seeded)

            if not seeded:
                raise RuntimeError("seed produced zero candles")

            # --- Now subscribe for live ticks ---
            stream = await client.subscribe_symbol(ASSET)

            async for tick in stream:
                if _stop.is_set():
                    break

                # Each tick is a dict with time and price fields.
                ts = int(tick.get("time") or tick.get("timestamp") or 0)
                if ts > 10_000_000_000:
                    ts //= 1000
                price = float(tick.get("close") or tick.get("price") or 0.0)
                if ts == 0 or price == 0.0:
                    continue

                # Bucket the tick into a 1-minute candle.
                bucket = (ts // TIMEFRAME) * TIMEFRAME

                with _lock:
                    last = _state["candles"][-1] if _state["candles"] else None
                    forming = _state["forming"]

                if forming is None or forming["time"] != bucket:
                    # New bucket. Close out whatever we had, open a new one.
                    new_forming = {
                        "time": bucket,
                        "open": price,
                        "high": price,
                        "low": price,
                        "close": price,
                    }

                    # Patch any seam between our last closed candle and
                    # this new bucket before we start the new forming bar.
                    if last and bucket - last["time"] > TIMEFRAME:
                        patch = await _fetch_gap(
                            client, last["time"], bucket
                        )
                        if patch:
                            merged = _dedupe_sorted(
                                _state["candles"] + patch
                            )
                            with _lock:
                                _state["candles"] = merged[-SEED_CANDLES:]
                                _state["gaps_patched"] += 1

                    with _lock:
                        # The old forming candle becomes a closed candle.
                        if forming is not None:
                            _state["candles"] = (
                                _state["candles"] + [forming]
                            )[-SEED_CANDLES:]
                        _state["forming"] = new_forming
                        _state["gaps_remaining"] = _count_gaps(
                            _state["candles"]
                        )
                else:
                    # Same bucket. Update the forming candle in place.
                    with _lock:
                        f = _state["forming"]
                        f["high"] = max(f["high"], price)
                        f["low"] = min(f["low"], price)
                        f["close"] = price

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
            "forming": dict(_state["forming"]) if _state["forming"] else None,
            "ticks": _state["ticks"],
            "gaps_patched": _state["gaps_patched"],
            "gaps_remaining": _state["gaps_remaining"],
            "last_tick_age_s": round(now - last, 2) if last else None,
            "uptime_s": round(now - _state["started_at"], 1),
        }
