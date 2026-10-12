"""
Multi-asset 10s candle builder for bandwidth comparison.

Three assets on one client, three tick subscriptions, three independent
candle builders. No seeding — 10s bars don't exist server-side and split
bars from 60s would produce fake bandwidth readings.

What "bandwidth" means here:

    bandwidth % = (upper - lower) / middle * 100

where upper/middle/lower are the Bollinger Bands (20-period, 2σ) over the
asset's recent 10s closes. It's the width of the band as a percentage of
the middle band — a normalised volatility measure. Because it's a
percentage, it's directly comparable across assets with very different
price scales (EURUSD ~1.08 vs EURGBP ~0.85 vs AUDUSD ~0.66).

The chart shows one line per asset on a single pane, all three on the
same axis, so you can see which is ranging wider right now.
"""

import asyncio
import os
import threading
import time

ASSETS = [
    a.strip()
    for a in os.getenv(
        "ASSETS", "EURGBP_otc,EURUSD_otc,AUDUSD_otc"
    ).split(",")
    if a.strip()
]
TIMEFRAME = int(os.getenv("TIMEFRAME", "10"))
MAX_CANDLES = int(os.getenv("MAX_CANDLES", "300"))
BB_PERIOD = int(os.getenv("BB_PERIOD", "20"))
BB_STD = float(os.getenv("BB_STD", "2.0"))
SSID = os.getenv("POCKET_OPTION_SSID")

# One state dict per asset.
_state = {
    asset: {
        "candles": [],
        "forming": None,
        "ticks": 0,
        "last_tick_ts": 0.0,
    }
    for asset in ASSETS
}

_conn = {
    "connected": False,
    "error": None,
    "started_at": time.time(),
}
_lock = threading.Lock()

_loop = None
_task = None
_stop = threading.Event()


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


def _ingest_tick(asset, ts, price):
    """
    Fold a tick into the asset's current 10s bucket.
    Called under _lock.
    """
    st = _state[asset]
    bucket = _bucket(ts)
    forming = st["forming"]

    if forming is None:
        st["forming"] = {
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
        st["candles"] = (st["candles"] + [dict(forming)])[-MAX_CANDLES:]
        st["forming"] = {
            "time": bucket,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
        }
        return

    # Late tick for a closed bucket — drop.
    return


def _bollinger(closes, period=BB_PERIOD, mult=BB_STD):
    if len(closes) < period:
        return None, None, None
    window = closes[-period:]
    mean = sum(window) / period
    variance = sum((x - mean) ** 2 for x in window) / period
    sd = variance ** 0.5
    return mean + mult * sd, mean, mean - mult * sd


def _bandwidth_pct(upper, middle, lower):
    if upper is None or middle is None or lower is None:
        return None
    if middle == 0:
        return None
    return (upper - lower) / middle * 100.0


async def _stream_asset(client, asset):
    """
    Independent tick consumer per asset. Runs forever until cancelled.
    """
    retry_delay = 1.0

    while not _stop.is_set():
        try:
            stream = await client.subscribe_symbol(asset)
            print(f"[{asset}] subscribed", flush=True)

            sample_logged = 0

            async for tick in stream:
                if _stop.is_set():
                    break

                if sample_logged < 3:
                    print(f"[{asset}] tick sample: {tick!r}", flush=True)
                    sample_logged += 1

                try:
                    ts, price = _extract_tick(tick)
                except Exception as e:
                    print(
                        f"[{asset}] tick skip {type(e).__name__}: {e}",
                        flush=True,
                    )
                    continue

                with _lock:
                    _ingest_tick(asset, ts, price)
                    _state[asset]["ticks"] += 1
                    _state[asset]["last_tick_ts"] = time.time()

            retry_delay = 1.0
            await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(
                f"[{asset}] stream error {type(e).__name__}: {e}",
                flush=True,
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30.0)


async def _stream():
    from BinaryOptionsToolsV2 import PocketOptionAsync
    from BinaryOptionsToolsV2.config import Config

    if not SSID:
        raise RuntimeError("POCKET_OPTION_SSID is not set")

    if len(ASSETS) > 4:
        raise RuntimeError(
            f"{len(ASSETS)} assets requested; the library supports at "
            f"most 4 concurrent subscriptions per client"
        )

    config = Config(
        max_allowed_loops=3,
        reconnect_time=15,
        connection_initialization_timeout_secs=30,
        terminal_logging=False,
    )

    client = PocketOptionAsync(SSID, config=config)
    await client.wait_for_assets(timeout=60.0)

    with _lock:
        _conn["connected"] = client.is_connected()
        _conn["error"] = None

    tasks = [
        asyncio.create_task(_stream_asset(client, a))
        for a in ASSETS
    ]

    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()
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
            _conn["error"] = f"loop: {type(e).__name__}: {e}"
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
        t = threading.Thread(
            target=_run_loop, daemon=True, name="stream"
        )
        t.start()


def snapshot():
    """
    Emit one series per asset, each with:
      - bandwidth_pct over closed+forming closes
      - current numeric readings (rsi/ema/bb) for the side panel
    """
    with _lock:
        now = time.time()
        assets_out = []

        for asset in ASSETS:
            st = _state[asset]
            closed = list(st["candles"])
            forming = dict(st["forming"]) if st["forming"] else None
            display = closed + ([forming] if forming else [])
            closes = [c["close"] for c in display]

            upper, middle, lower = _bollinger(closes)
            bw = _bandwidth_pct(upper, middle, lower)

            # Build a full bandwidth series by walking the closes,
            # computing BB at each step once enough history exists.
            bw_series = []
            if len(closes) >= BB_PERIOD:
                for i in range(BB_PERIOD - 1, len(closes)):
                    sub = closes[: i + 1]
                    u, m, l = _bollinger(sub)
                    b = _bandwidth_pct(u, m, l)
                    if b is None:
                        continue
                    bw_series.append(
                        {"time": display[i]["time"], "value": round(b, 6)}
                    )

            assets_out.append(
                {
                    "asset": asset,
                    "candles": display,
                    "closed_count": len(closed),
                    "forming": forming,
                    "ticks": st["ticks"],
                    "last_tick_ts": st["last_tick_ts"],
                    "bandwidth_pct": (
                        round(bw, 6) if bw is not None else None
                    ),
                    "bb_upper": upper,
                    "bb_middle": middle,
                    "bb_lower": lower,
                    "bandwidth_series": bw_series,
                }
            )

        return {
            "assets": assets_out,
            "timeframe": TIMEFRAME,
            "bb_period": BB_PERIOD,
            "bb_std": BB_STD,
            "connected": _conn["connected"],
            "error": _conn["error"],
            "last_tick_age_s": round(
                now - max(
                    (_state[a]["last_tick_ts"] for a in ASSETS),
                    default=0.0,
                ),
                2,
            ),
            "uptime_s": round(now - _conn["started_at"], 1),
        }
