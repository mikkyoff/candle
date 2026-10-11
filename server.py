"""
Single-asset stream worker.

Owns one PocketOptionAsync client and one subscription to ASSET.
get_candles_live() yields (closed_candles, forming_candle) on every
tick, which is exactly the shape the chart consumes.

The library handles:
  - tick subscription buffering before backfill
  - merging advanced/history/compiled candle sources
  - bucket-aligned candle close
  - socket-level reconnect

We only need to:
  - hold the latest snapshot in memory for the HTTP layer
  - restart the generator if it ever ends or raises
"""

import asyncio
import os
import threading
import time

ASSET = os.getenv("ASSET", "EURJPY_otc")
TIMEFRAME = int(os.getenv("TIMEFRAME", "60"))
SEED_CANDLES = int(os.getenv("SEED_CANDLES", "150"))
SSID = os.getenv("POCKET_OPTION_SSID")

# Library returns each closed candle with 'time' (int, seconds) and OHLC.
# We keep the last SEED_CANDLES here. No indicator math in this file —
# the browser computes RSI/EMA/BB from the same series the chart draws,
# so the numbers and the lines can never disagree.

_state = {
    "candles": [],          # list[dict], oldest first, closed only
    "forming": None,        # dict | None, current in-progress candle
    "connected": False,
    "error": None,
    "seeded": False,
    "ticks": 0,
    "last_tick_ts": 0.0,
    "started_at": time.time(),
}
_lock = threading.Lock()

_loop = None
_task = None
_stop = threading.Event()


def _normalize(c):
    """Coerce a library candle into the shape Lightweight Charts wants."""
    return {
        "time": int(c["time"]),
        "open": float(c["open"]),
        "high": float(c["high"]),
        "low": float(c["low"]),
        "close": float(c["close"]),
    }


def _dedupe_sorted(candles):
    """Sort by timestamp, drop duplicates (library merges multiple sources)."""
    seen = set()
    out = []
    for c in sorted(candles, key=lambda x: x["time"]):
        if c["time"] in seen:
            continue
        seen.add(c["time"])
        out.append(c)
    return out


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
            gen = client.get_candles_live(
                ASSET,
                period=TIMEFRAME,
                hours=SEED_CANDLES * TIMEFRAME / 3600.0,
                max_rows=SEED_CANDLES,
            )

            first_yield = True

            async for closed, forming in gen:
                if _stop.is_set():
                    break

                parsed_closed = [_normalize(c) for c in (closed or [])]
                parsed_closed = _dedupe_sorted(parsed_closed)
                parsed_forming = _normalize(forming) if forming else None

                with _lock:
                    if first_yield:
                        # Keep only the tail we display.
                        _state["candles"] = parsed_closed[-SEED_CANDLES:]
                        _state["seeded"] = True
                        first_yield = False
                    else:
                        _state["candles"] = parsed_closed[-SEED_CANDLES:]

                    _state["forming"] = parsed_forming
                    _state["connected"] = client.is_connected()
                    _state["ticks"] += 1
                    _state["last_tick_ts"] = time.time()
                    _state["error"] = None

            # Generator ended cleanly — restart after a beat.
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
    """Idempotent. Spawns the worker thread on first call."""
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
            "error": _state["error"],
            "candles": list(_state["candles"]),
            "forming": dict(_state["forming"]) if _state["forming"] else None,
            "ticks": _state["ticks"],
            "last_tick_age_s": round(now - last, 2) if last else None,
            "uptime_s": round(now - _state["started_at"], 1),
        }
