"""
Multi-asset 10s candle builder for bandwidth comparison.

Three assets on one client, three tick subscriptions, three independent
candle builders. No seeding — 10s bars don't exist server-side.

The library's subscribe_symbol() emits the server's *in-progress candle*
on every price change, not a raw price tick. Shape:

    {'symbol': 'EURUSD_otc', 'timestamp': 1791780799,
     'open': '1.09908', 'high': '1.09908', 'low': '1.09908',
     'close': '1.09908', 'volume': None, 'is_closed': False}

So we treat `close` as the current price and `timestamp` as the tick
time, then bucket into 10s candles ourselves.
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

_state = {
    asset: {
        "candles": [],
        "forming": None,
        "ticks": 0,
        "last_tick_ts": 0.0,
        "sample": None,          # last raw tick, for debugging
        "sub_error": None,
    }
    for asset in ASSETS
}

_conn = {
    "connected": False,
    "error": None,
    "started_at": time.time(),
    "snapshots": 0,
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


def _bollinger_window(window, mult=BB_STD):
    n = len(window)
    mean = sum(window) / n
    variance = sum((x - mean) ** 2 for x in window) / n
    sd = variance ** 0.5
    return mean + mult * sd, mean, mean - mult * sd


def _bandwidth_pct(upper, middle, lower):
    if upper is None or middle is None or lower is None:
        return None
    if middle == 0:
        return None
    return (upper - lower) / middle * 100.0


def _build_bandwidth_series(display, period, mult):
    """
    Compute the bandwidth series in one pass.

    For each index i where we have `period` closes, compute BB over
    closes[i-period+1 .. i] and emit a point at display[i]['time'].
    O(N * period) — much cheaper than the previous O(N^2).
    """
    if len(display) < period:
        return []

    closes = [c["close"] for c in display]
    out = []
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1 : i + 1]
        u, m, l = _bollinger_window(window, mult)
        b = _bandwidth_pct(u, m, l)
        if b is None:
            continue
        out.append({"time": display[i]["time"], "value": round(b, 6)})
    return out


async def _stream_asset(client, asset):
    retry_delay = 1.0

    while not _stop.is_set():
        try:
            stream = await client.subscribe_symbol(asset)
            print(f"[{asset}] subscribed", flush=True)

            async for tick in stream:
                if _stop.is_set():
                    break

                with _lock:
                    if _state[asset]["sample"] is None:
                        _state[asset]["sample"] = tick

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
            with _lock:
                _state[asset]["sub_error"] = (
                    f"{type(e).__name__}: {e}"
                )
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
            f"{len(ASSETS)} assets requested; max is 4 per client"
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
        asyncio.create_task(_stream_asset(client, a)) for a in ASSETS
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
    with _lock:
        _conn["snapshots"] += 1
        now = time.time()
        assets_out = []

        for asset in ASSETS:
            st = _state[asset]
            closed = list(st["candles"])
            forming = dict(st["forming"]) if st["forming"] else None
            display = closed + ([forming] if forming else [])
            closes = [c["close"] for c in display]

            if len(closes) >= BB_PERIOD:
                window = closes[-BB_PERIOD:]
                upper, middle, lower = _bollinger_window(window)
            else:
                upper = middle = lower = None

            bw = _bandwidth_pct(upper, middle, lower)
            bw_series = _build_bandwidth_series(
                display, BB_PERIOD, BB_STD
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
                    "sub_error": st["sub_error"],
                }
            )

        ages = [
            _state[a]["last_tick_ts"]
            for a in ASSETS
            if _state[a]["last_tick_ts"] > 0
        ]
        last_age = (
            round(now - max(ages), 2) if ages else None
        )

        return {
            "assets": assets_out,
            "timeframe": TIMEFRAME,
            "bb_period": BB_PERIOD,
            "bb_std": BB_STD,
            "connected": _conn["connected"],
            "error": _conn["error"],
            "last_tick_age_s": last_age,
            "uptime_s": round(now - _conn["started_at"], 1),
            "snapshots": _conn["snapshots"],
        }
