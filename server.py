"""
EURCHF_otc — 1 minute candles.

Two phases, per the developer's 1000candles.py example:

  1. Seed: subscribe_symbol() to open the history gate, wait 5s for it
     to open, then paginate get_candles_advanced() backward in pages of
     PAGE_SIZE candles. Small pages, no timeout wrapper, exact count.

  2. Stream: subscribe_symbol() ticks, bucketed into 60s candles by us.
     The forming candle updates in real time; a closed candle is
     emitted the instant a tick lands in the next minute bucket.

Bucket boundary:
    bucket(t) = (t // 60) * 60

The chart receives:
    candles : closed series, oldest first
    forming : the current in-progress 1m candle, updated per tick
"""

import asyncio
import os
import threading
import time

ASSET = os.getenv("ASSET", "EURCHF_otc")
TIMEFRAME = int(os.getenv("TIMEFRAME", "60"))
SEED_CANDLES = int(os.getenv("SEED_CANDLES", "150"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "50"))
MAX_CANDLES = int(os.getenv("MAX_CANDLES", "300"))
WARMUP_S = float(os.getenv("WARMUP_S", "5"))
SSID = os.getenv("POCKET_OPTION_SSID")

_state = {
    "candles": [],
    "forming": None,
    "connected": False,
    "error": None,
    "seeded": False,
    "seed_count": 0,
    "ticks": 0,
    "last_tick_ts": 0.0,
    "started_at": time.time(),
    "gaps_remaining": 0,
}
_lock = threading.Lock()

_loop = None
_task = None
_stop = threading.Event()


def _extract_ts(c):
    ts = c.get("time")
    if ts is None:
        ts = c.get("timestamp")
    if ts is None:
        raise KeyError("candle has neither 'time' nor 'timestamp'")
    ts = int(float(ts))
    if ts > 10_000_000_000:
        ts //= 1000
    return ts


def _normalize(c):
    return {
        "time": _extract_ts(c),
        "open": float(c["open"]),
        "high": float(c["high"]),
        "low": float(c["low"]),
        "close": float(c["close"]),
    }


def _extract_tick(tick):
    ts = tick.get("time")
    if ts is None:
        ts = tick.get("timestamp")
    if ts is None:
        raise KeyError("tick has neither 'time' nor 'timestamp'")
    ts = int(float(ts))
    if ts > 10_000_000_000:
        ts //= 1000

    price = tick.get("close")
    if price is None:
        price = tick.get("price")
    if price is None:
        raise KeyError("tick has neither 'close' nor 'price'")
    return ts, float(price)


def _bucket(ts):
    return (ts // TIMEFRAME) * TIMEFRAME


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
    Paginated backfill, per the developer's example.

    subscribe_symbol() opens the history gate; sleep() lets it open.
    Then walk server_time backward in PAGE_SIZE-candle steps.
    """
    await client.subscribe_symbol(ASSET)
    await asyncio.sleep(WARMUP_S)

    server_time = await client.get_server_time()

    pages = max(1, SEED_CANDLES // PAGE_SIZE)
    total = []

    for i in range(pages):
        server_time -= PAGE_SIZE * TIMEFRAME
        if server_time < 0:
            break

        try:
            page = await client.get_candles_advanced(
                ASSET,
                TIMEFRAME,
                PAGE_SIZE * TIMEFRAME,
                server_time,
            )
        except Exception as e:
            print(
                f"[seed] page {i} failed: {type(e).__name__}: {e}",
                flush=True,
            )
            continue

        if not page:
            print(f"[seed] page {i} empty — stopping", flush=True)
            break

        parsed = []
        for c in page:
            try:
                parsed.append(_normalize(c))
            except Exception:
                continue

        total = parsed + total
        print(
            f"[seed] page {i}: +{len(parsed)} (total {len(total)})",
            flush=True,
        )

        # Polite pause between pages.
        await asyncio.sleep(0.3)

    deduped = _dedupe_sorted(total)
    return deduped[-SEED_CANDLES:]


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
        closed = dict(forming)
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

    # Late tick for an already-closed bucket — drop.
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
            # --- Seed ---
            with _lock:
                _state["error"] = None

            try:
                seeded = await _seed(client)
            except Exception as e:
                with _lock:
                    _state["error"] = f"seed failed: {type(e).__name__}: {e}"
                raise

            with _lock:
                _state["candles"] = seeded
                _state["seeded"] = True
                _state["seed_count"] = len(seeded)
                _state["gaps_remaining"] = _count_gaps(seeded)

            print(f"[seed] final: {len(seeded)} candles", flush=True)

            # --- Stream ticks ---
            stream = await client.subscribe_symbol(ASSET)

            with _lock:
                _state["connected"] = client.is_connected()

            async for tick in stream:
                if _stop.is_set():
                    break

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
            "seeded": _state["seeded"],
            "seed_count": _state["seed_count"],
            "candles": list(_state["candles"]),
            "forming": (
                dict(_state["forming"]) if _state["forming"] else None
            ),
            "ticks": _state["ticks"],
            "gaps_remaining": _state["gaps_remaining"],
            "last_tick_age_s": round(now - last, 2) if last else None,
            "uptime_s": round(now - _state["started_at"], 1),
        }
