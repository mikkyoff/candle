"""
Live 15s candles for EURCHF_otc, built from raw ticks.

subscribe_symbol(asset) yields raw price updates. We bucket them into
15s candles ourselves, so:

  - the forming candle updates in real time (no waiting for close),
  - bucket boundaries are ours (:00/:15/:30/:45 of the wall clock),
  - a closed candle is emitted the instant a tick lands in the next bucket.

Time alignment:
  bucket_start(t) = (t // 15) * 15

The chart receives both:
  - `candles` : the closed series (immutable once written)
  - `forming` : the current in-progress candle, updated on every tick
"""

import asyncio
import os
import threading
import time

ASSET = os.getenv("ASSET", "EURCHF_otc")
TIMEFRAME = int(os.getenv("TIMEFRAME", "15"))
MAX_CANDLES = int(os.getenv("MAX_CANDLES", "150"))
SSID = os.getenv("POCKET_OPTION_SSID")

_state = {
    "candles": [],           # closed only, oldest first
    "forming": None,         # dict | None
    "connected": False,
    "error": None,
    "ticks": 0,
    "last_tick_ts": 0.0,
    "started_at": time.time(),
    "gaps_remaining": 0,
    "last_raw_tick": None,   # for debugging
}
_lock = threading.Lock()

_loop = None
_task = None
_stop = threading.Event()


def _extract_tick(tick):
    """
    Pull (timestamp_seconds, price) out of a raw tick dict.

    Different code paths in the library serialize the time field
    differently ('time' vs 'timestamp') and the price field
    differently ('close' vs 'price'). Accept any combination.
    """
    ts = tick.get("time")
    if ts is None:
        ts = tick.get("timestamp")
    if ts is None:
        raise KeyError("tick has neither 'time' nor 'timestamp'")

    ts = int(float(ts))
    if ts > 10_000_000_000:      # milliseconds -> seconds
        ts //= 1000

    price = tick.get("close")
    if price is None:
        price = tick.get("price")
    if price is None:
        raise KeyError("tick has neither 'close' nor 'price'")

    return ts, float(price)


def _bucket(ts):
    """Start-of-bucket timestamp for a given tick time."""
    return (ts // TIMEFRAME) * TIMEFRAME


def _count_gaps(candles):
    if len(candles) < 2:
        return 0
    gaps = 0
    for i in range(1, len(candles)):
        delta = candles[i]["time"] - candles[i - 1]["time"]
        if delta > TIMEFRAME:
            gaps += delta // TIMEFRAME - 1
    return gaps


def _close_forming(state_forming):
    """Return a copy of the forming candle suitable for the closed list."""
    return dict(state_forming)


def _ingest_tick(ts, price):
    """
    Fold one tick into the current bucket, or start a new bucket.

    Called under _lock.
    """
    bucket = _bucket(ts)
    forming = _state["forming"]

    if forming is None:
        _state["forming"] = {
            "time": bucket,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
        }
        return

    if bucket == forming["time"]:
        if price > forming["high"]:
            forming["high"] = price
        if price < forming["low"]:
            forming["low"] = price
        forming["close"] = price
        return

    if bucket > forming["time"]:
        # Previous bucket is done — close it and start the new one.
        closed = _close_forming(forming)
        _state["candles"] = (_state["candles"] + [closed])[-MAX_CANDLES:]
        _state["forming"] = {
            "time": bucket,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
        }
        _state["gaps_remaining"] = _count_gaps(_state["candles"])
        return

    # bucket < forming["time"]: a late tick for an already-closed bucket.
    # Drop it. We do not retroactively rewrite closed candles.
    return


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
    sample_logged = 0

    while not _stop.is_set():
        try:
            stream = await client.subscribe_symbol(ASSET)

            with _lock:
                _state["connected"] = client.is_connected()
                _state["error"] = None

            async for tick in stream:
                if _stop.is_set():
                    break

                # Log the first few raw ticks so the shape is visible
                # in Railway logs if the field names ever change again.
                if sample_logged < 5:
                    print(f"[tick sample] {tick!r}", flush=True)
                    sample_logged += 1

                try:
                    ts, price = _extract_tick(tick)
                except Exception as e:
                    print(
                        f"[tick skip] {type(e).__name__}: {e} "
                        f"— tick was {tick!r}",
                        flush=True,
                    )
                    continue

                with _lock:
                    _ingest_tick(ts, price)
                    _state["ticks"] += 1
                    _state["last_tick_ts"] = time.time()
                    _state["connected"] = client.is_connected()
                    _state["last_raw_tick"] = tick

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
            "forming": (
                dict(_state["forming"]) if _state["forming"] else None
            ),
            "ticks": _state["ticks"],
            "gaps_remaining": _state["gaps_remaining"],
            "last_tick_age_s": round(now - last, 2) if last else None,
            "uptime_s": round(now - _state["started_at"], 1),
        }
