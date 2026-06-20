"""
Deriv SMC OB+FVG Scanner
Pairs: R_75, 1HZ75V, R_10, R_25
Stack: H4 (EMA/ADX bias) >> H1 (Order Block + BOS) >> M15 (FVG nested in OB) >> M5 (entry trigger)
Logic:
  - H4 EMA21/EMA50 bias + ADX strength filter
  - H1: most recent order block (last opposite-colour candle) before a structure-breaking
    impulse move that matches H4 bias
  - M15: Fair Value Gap (3-candle imbalance) that overlaps ("nests inside") the H1 OB —
    the overlap region becomes the entry zone
  - M5: rejection wick into the zone + RSI confirmation + momentum body filter = entry trigger
  - Zone expires if no M5 trigger within zone_max_age_h1_bars
  - File-based cooldown, committed back to the repo by the GitHub Actions workflow
  - Single-pass architecture — this script runs once per invocation. The GitHub Actions
    cron handles the repeating schedule, so there is no LIVE_MODE loop here on purpose
    (running this continuously on Railway *and* on a GH Actions cron at the same time
    would double-fire alerts, the same issue the Scalper had).
  - HTML Telegram alerts
"""

import asyncio, json, logging, os, time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
import urllib.request, urllib.parse
import pandas as pd
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
WAT = timezone(timedelta(hours=1))


# =============================================================================
#  CONFIG
# =============================================================================
@dataclass
class Config:
    symbols: list = field(default_factory=lambda: [
        "R_75", "1HZ75V", "R_10", "R_25"
    ])

    tg_token:   str = ""
    tg_chat_id: str = ""

    # Timeframes (seconds)
    h4_tf:  int = 14400
    h1_tf:  int = 3600
    m15_tf: int = 900
    m5_tf:  int = 300

    h4_count:  int = 60
    h1_count:  int = 110
    m15_count: int = 220
    m5_count:  int = 220

    # H4 bias
    adx_period: int   = 14
    adx_min:    float = 18.0
    atr_period: int   = 14

    # H1 order block / structure break
    structure_break_lookback: int   = 10   # swing lookback for BOS
    ob_lookback:              int   = 20   # how many H1 bars back to search for a valid OB
    ob_impulse_body_min:      float = 0.5  # min body ratio for the breakout/impulse candle

    # M15 FVG
    fvg_search_window_h1_bars: int   = 20    # how far past the OB to look for a nested FVG
    fvg_min_gap_pct:           float = 0.02  # min gap size as % of price, filters noise

    # M5 entry trigger
    rsi_period:        int   = 14
    rsi_bull_min:       float = 45.0   # bull entries need RSI above this (momentum not exhausted)
    rsi_bear_max:       float = 55.0   # bear entries need RSI below this
    min_wick_ratio:     float = 0.25
    min_momentum_body:  float = 0.30
    zone_max_age_h1_bars: int = 12     # zone expires if untouched after this many H1 bars

    # Trade plan
    rr_min: float = 1.5
    rr_max: float = 3.0
    zone_sl_buffer_mult: float = 0.15  # extra SL buffer, as a fraction of zone width

    cooldown_hours: int = 6
    live_mode: bool = False  # intentionally off by default; see module docstring

    @property
    def uri(self):
        return "wss://ws.derivws.com/websockets/v3?app_id=1089"

CFG = Config()

if os.environ.get("TG_TOKEN"):   CFG.tg_token   = os.environ["TG_TOKEN"]
if os.environ.get("TG_CHAT_ID"): CFG.tg_chat_id = os.environ["TG_CHAT_ID"]
if os.environ.get("LIVE_MODE"):  CFG.live_mode  = os.environ["LIVE_MODE"].strip().lower() in ("1", "true", "yes")


# =============================================================================
#  COOLDOWN
# =============================================================================
COOLDOWN_FILE = "smc_cooldown.json"

def load_cooldown():
    try:
        if os.path.exists(COOLDOWN_FILE):
            with open(COOLDOWN_FILE) as f: return json.load(f)
    except Exception: pass
    return {}

def save_cooldown(data):
    try:
        with open(COOLDOWN_FILE, "w") as f: json.dump(data, f)
    except Exception as e: log.error("Cooldown save: %s", e)

def is_duplicate(symbol, tag, zone_low, zone_high):
    key = f"{symbol}_{tag}_{round(zone_low, 2)}_{round(zone_high, 2)}"
    cd  = load_cooldown()
    if key in cd:
        hrs = (time.time() - cd[key]) / 3600
        if hrs < CFG.cooldown_hours:
            log.info("Duplicate: %s (%.1fh ago)", key, hrs)
            return True
    return False

def mark_sent(symbol, tag, zone_low, zone_high):
    key = f"{symbol}_{tag}_{round(zone_low, 2)}_{round(zone_high, 2)}"
    cd  = load_cooldown()
    now = time.time()
    cd  = {k: v for k, v in cd.items() if now - v < 86400}
    cd[key] = now
    save_cooldown(cd)


# =============================================================================
#  TELEGRAM
# =============================================================================
def send_telegram(message):
    if not CFG.tg_token or not CFG.tg_chat_id:
        log.info("Telegram not configured.")
        return
    try:
        url  = f"https://api.telegram.org/bot{CFG.tg_token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": CFG.tg_chat_id, "text": message, "parse_mode": "HTML"
        }).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10)
        log.info("Telegram sent.")
    except Exception as e: log.error("Telegram error: %s", e)


def build_zone_headsup(symbol, direction, ob, fvg, h4_bias, now):
    icon = "📈" if direction == "BULL" else "📉"
    dt   = "BULLISH" if direction == "BULL" else "BEARISH"
    return (
        f"{icon} <b>{dt} SMC ZONE — {symbol}</b>\n"
        f"<b>H4 Bias:</b>   {h4_bias}\n"
        f"<b>OB Zone:</b>   {ob['ob_low']} – {ob['ob_high']}\n"
        f"<b>FVG Zone:</b>  {fvg['zone_low']} – {fvg['zone_high']}\n"
        f"<b>Time:</b>      {now}\n"
        f"<i>OB + FVG confluence confirmed. Watching for M5 entry trigger...</i>"
    )


def build_alert(symbol, direction, zone_low, zone_high,
                entry, sl, tp1, tp2, risk, rr,
                ob_bar, fvg_bar, m5_bar, wick_ratio, rsi_val, score, rating, h4_bias):
    icon   = "🟢 <b>BULLISH SMC ENTRY</b>" if direction == "BULL" else "🔴 <b>BEARISH SMC ENTRY</b>"
    action = "BUY on M5 confirmation" if direction == "BULL" else "SELL on M5 confirmation"
    obt    = ob_bar.astimezone(WAT).strftime("%m-%d %H:%M WAT")
    fvt    = fvg_bar.astimezone(WAT).strftime("%m-%d %H:%M WAT")
    mt     = m5_bar.astimezone(WAT).strftime("%m-%d %H:%M WAT")
    stars  = "🔥 PRIME" if rating=="PRIME" else "⭐⭐ STRONG" if rating=="STRONG" else "⭐ GOOD" if rating=="GOOD" else "✗ SKIP"
    return (
        f"{icon}\n"
        f"&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;\n"
        f"<b>Pair:</b>    {symbol}\n"
        f"<b>Action:</b>  {action}\n"
        f"<b>Rating:</b>  {stars}  ({score}/6)\n"
        f"<b>H4 Bias:</b> {h4_bias}\n"
        f"&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;\n"
        f"<b>Zone:</b>        {zone_low} – {zone_high}\n"
        f"<b>H1 OB:</b>       {obt}\n"
        f"<b>M15 FVG:</b>     {fvt}\n"
        f"<b>M5 Trigger:</b>  {mt}  (wick {wick_ratio}, RSI {rsi_val})\n"
        f"&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;\n"
        f"<b>Entry:</b>   {entry}\n"
        f"<b>SL:</b>      {sl}\n"
        f"<b>TP1:</b>     {tp1}  <i>(close 50%, move SL to BE)</i>\n"
        f"<b>TP2:</b>     {tp2}  <i>(let rest run  1:{rr})</i>\n"
        f"<b>Risk/pt:</b> {risk}\n"
        f"&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;&#8212;\n"
        f"<i>H4 bias + H1 OB + M15 FVG nested + M5 trigger confirmed</i>"
    )


# =============================================================================
#  WEBSOCKET
# =============================================================================
async def fetch_candles(ws, symbol, granularity, count):
    await ws.send(json.dumps({
        "ticks_history": symbol, "adjust_start_time": 1,
        "count": count, "end": "latest", "style": "candles", "granularity": granularity,
    }))
    resp = json.loads(await ws.recv())
    if "error" in resp or not resp.get("candles"):
        log.error("%s fetch error (%ds): %s", symbol, granularity,
                  resp.get("error", {}).get("message", "no data"))
        return None
    df = pd.DataFrame(resp["candles"])
    df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close"}, inplace=True)
    df[["Open","High","Low","Close"]] = df[["Open","High","Low","Close"]].apply(
        pd.to_numeric, errors="coerce")
    df["Time"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    df.set_index("Time", inplace=True)
    df.drop(columns=["epoch"], inplace=True)
    return df


async def fetch_symbol_data(symbol):
    try:
        async with websockets.connect(CFG.uri, ping_timeout=15) as ws:
            h4  = await fetch_candles(ws, symbol, CFG.h4_tf,  CFG.h4_count)
            h1  = await fetch_candles(ws, symbol, CFG.h1_tf,  CFG.h1_count)
            m15 = await fetch_candles(ws, symbol, CFG.m15_tf, CFG.m15_count)
            m5  = await fetch_candles(ws, symbol, CFG.m5_tf,  CFG.m5_count)
            return h4, h1, m15, m5
    except (websockets.exceptions.WebSocketException, asyncio.TimeoutError) as e:
        log.error("%s connection error: %s", symbol, e)
        return None, None, None, None


def closed_only(df):
    """Drop the still-forming last candle so every detector only ever sees closed bars."""
    return df.iloc[:-1] if df is not None and len(df) > 1 else df


# =============================================================================
#  INDICATORS
# =============================================================================
def ema(s, n): return s.ewm(span=n, adjust=False).mean()

def atr_s(df, n=14):
    tr = pd.concat([df["High"]-df["Low"],
                    (df["High"]-df["Close"].shift()).abs(),
                    (df["Low"] -df["Close"].shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()

def body_ratio(df):
    rng = (df["High"] - df["Low"]).replace(0, float("nan"))
    return (df["Close"] - df["Open"]).abs() / rng

def calc_adx(df, n=14):
    up   = df["High"].diff()
    down = -df["Low"].diff()
    pdm  = pd.Series(0.0, index=df.index)
    ndm  = pd.Series(0.0, index=df.index)
    pdm[up > down] = up[up > down].clip(lower=0)
    ndm[down > up] = down[down > up].clip(lower=0)
    atr_ = atr_s(df, n)
    pdi  = 100 * pdm.ewm(span=n, adjust=False).mean() / atr_.replace(0, float("nan"))
    ndi  = 100 * ndm.ewm(span=n, adjust=False).mean() / atr_.replace(0, float("nan"))
    dx   = 100 * (pdi-ndi).abs() / (pdi+ndi).replace(0, float("nan"))
    return dx.ewm(span=n, adjust=False).mean()

def calc_rsi(df, n=14):
    delta    = df["Close"].diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/n, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/n, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


# =============================================================================
#  H4 BIAS
# =============================================================================
def get_h4_bias(h4_closed):
    df = h4_closed.copy()
    df["E21"] = ema(df["Close"], 21)
    df["E50"] = ema(df["Close"], 50)
    df["ADX"] = calc_adx(df, CFG.adx_period)
    a, b = df.iloc[-1], df.iloc[-3]
    adx_val = float(a["ADX"])
    if a["E21"] > a["E50"] and a["Close"] > a["E21"] and a["E21"] > b["E21"]:
        return "BULLISH", adx_val
    if a["E21"] < a["E50"] and a["Close"] < a["E21"] and a["E21"] < b["E21"]:
        return "BEARISH", adx_val
    return "NEUTRAL", adx_val


# =============================================================================
#  H1 ORDER BLOCK + STRUCTURE BREAK
# =============================================================================
def find_h1_order_block(h1_closed):
    df = h1_closed.copy()
    df["BR"]  = body_ratio(df)
    df["ATR"] = atr_s(df, CFG.atr_period)
    df["ADX"] = calc_adx(df, CFG.adx_period)

    n = CFG.structure_break_lookback
    df["SwingHigh"] = df["High"].shift(1).rolling(n).max()
    df["SwingLow"]  = df["Low"].shift(1).rolling(n).min()

    lookback = min(CFG.ob_lookback, len(df) - n - 2)
    if lookback < 3:
        return {"direction": None}

    start = len(df) - lookback

    for i in range(len(df) - 1, start - 1, -1):
        row = df.iloc[i]
        if pd.isna(row["SwingHigh"]) or pd.isna(row["SwingLow"]):
            continue

        # Bullish impulse: closes above prior swing high, strong body, breaks structure
        if (row["Close"] > row["SwingHigh"] and row["Close"] > row["Open"]
                and row["BR"] >= CFG.ob_impulse_body_min):
            ob_idx = None
            for j in range(i - 1, max(i - 4, -1), -1):
                cand = df.iloc[j]
                if cand["Close"] < cand["Open"]:
                    ob_idx = j
                    break
            if ob_idx is not None:
                ob = df.iloc[ob_idx]
                return {
                    "direction": "BULL",
                    "ob_low": float(ob["Low"]), "ob_high": float(ob["High"]),
                    "ob_bar": ob.name, "impulse_bar": row.name,
                    "impulse_br": float(row["BR"]), "adx": float(row["ADX"]),
                }

        # Bearish impulse: closes below prior swing low, strong body, breaks structure
        if (row["Close"] < row["SwingLow"] and row["Close"] < row["Open"]
                and row["BR"] >= CFG.ob_impulse_body_min):
            ob_idx = None
            for j in range(i - 1, max(i - 4, -1), -1):
                cand = df.iloc[j]
                if cand["Close"] > cand["Open"]:
                    ob_idx = j
                    break
            if ob_idx is not None:
                ob = df.iloc[ob_idx]
                return {
                    "direction": "BEAR",
                    "ob_low": float(ob["Low"]), "ob_high": float(ob["High"]),
                    "ob_bar": ob.name, "impulse_bar": row.name,
                    "impulse_br": float(row["BR"]), "adx": float(row["ADX"]),
                }

    return {"direction": None}


# =============================================================================
#  M15 FVG NESTED INSIDE THE H1 OB
# =============================================================================
def find_m15_fvg_nested(m15_closed, ob):
    if not ob.get("direction"):
        return {"fvg": False}

    direction = ob["direction"]
    ob_low, ob_high = ob["ob_low"], ob["ob_high"]

    window_start = ob["ob_bar"] + pd.Timedelta(seconds=CFG.h1_tf)
    window_end   = ob["ob_bar"] + pd.Timedelta(seconds=CFG.h1_tf * (1 + CFG.fvg_search_window_h1_bars))

    df = m15_closed[(m15_closed.index >= window_start) & (m15_closed.index <= window_end)]
    if len(df) < 3:
        return {"fvg": False}

    for k in range(len(df) - 2):
        c1, c3 = df.iloc[k], df.iloc[k + 2]

        if direction == "BULL" and c1["High"] < c3["Low"]:
            gap_low, gap_high = float(c1["High"]), float(c3["Low"])
        elif direction == "BEAR" and c1["Low"] > c3["High"]:
            gap_low, gap_high = float(c3["High"]), float(c1["Low"])
        else:
            continue

        if (gap_high - gap_low) < gap_low * (CFG.fvg_min_gap_pct / 100):
            continue

        zone_low  = max(ob_low,  gap_low)
        zone_high = min(ob_high, gap_high)
        if zone_low < zone_high:
            return {
                "fvg": True, "fvg_bar": c3.name,
                "zone_low": round(zone_low, 4), "zone_high": round(zone_high, 4),
            }

    return {"fvg": False}


# =============================================================================
#  M5 ENTRY TRIGGER
# =============================================================================
def find_m5_trigger(m5_closed, ob, fvg):
    if not fvg.get("fvg"):
        return {"trigger": False, "expired": False}

    direction = ob["direction"]
    zone_low, zone_high = fvg["zone_low"], fvg["zone_high"]
    level = zone_high if direction == "BULL" else zone_low

    full = m5_closed.copy()
    full["RSI"] = calc_rsi(full, CFG.rsi_period)
    full["BR"]  = body_ratio(full)

    window_start = fvg["fvg_bar"] + pd.Timedelta(seconds=CFG.m15_tf)
    window_end   = ob["ob_bar"] + pd.Timedelta(seconds=CFG.h1_tf * (1 + CFG.zone_max_age_h1_bars))
    df = full[(full.index >= window_start) & (full.index <= window_end)]

    for _, row in df.iloc[::-1].iterrows():
        rng = row["High"] - row["Low"]
        if rng <= 0 or pd.isna(row["RSI"]):
            continue

        if direction == "BULL":
            touches = row["Low"] <= zone_high and row["High"] >= zone_low
            rejects = row["Close"] > level
            wick    = level - row["Low"]
        else:
            touches = row["High"] >= zone_low and row["Low"] <= zone_high
            rejects = row["Close"] < level
            wick    = row["High"] - level

        if not (touches and rejects) or wick <= 0:
            continue

        wick_ratio = wick / rng
        if wick_ratio < CFG.min_wick_ratio:
            continue
        if row["BR"] < CFG.min_momentum_body:
            continue
        if direction == "BULL" and row["RSI"] < CFG.rsi_bull_min:
            continue
        if direction == "BEAR" and row["RSI"] > CFG.rsi_bear_max:
            continue

        return {
            "trigger": True, "trigger_bar": row.name,
            "entry": round(float(row["Close"]), 4),
            "wick_ratio": round(wick_ratio, 3),
            "rsi": round(float(row["RSI"]), 1),
        }

    now_utc = pd.Timestamp.now(tz="UTC")
    return {"trigger": False, "expired": now_utc > window_end}


# =============================================================================
#  TRADE PLAN
# =============================================================================
def build_trade(ob, fvg, trigger, score):
    if not trigger.get("trigger"):
        return {}

    direction = ob["direction"]
    entry     = trigger["entry"]
    zone_low, zone_high = fvg["zone_low"], fvg["zone_high"]
    buf       = (zone_high - zone_low) * CFG.zone_sl_buffer_mult
    rr        = round(CFG.rr_min + (score / 6) * (CFG.rr_max - CFG.rr_min), 2)

    if direction == "BULL":
        sl   = round(zone_low - buf, 4)
        risk = round(entry - sl, 4)
        tp1  = round(entry + risk * 1.0, 4)
        tp2  = round(entry + risk * rr,  4)
    else:
        sl   = round(zone_high + buf, 4)
        risk = round(sl - entry, 4)
        tp1  = round(entry - risk * 1.0, 4)
        tp2  = round(entry - risk * rr,  4)

    return {"entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "risk": risk, "rr": rr}


def score_signal(ob, fvg, trigger):
    score = 1
    if ob["adx"] >= CFG.adx_min:                          score += 1
    if ob["impulse_br"] >= 0.6:                            score += 1
    if trigger["wick_ratio"] >= 0.4:                       score += 1
    if (ob["direction"] == "BULL" and trigger["rsi"] >= 55) or \
       (ob["direction"] == "BEAR" and trigger["rsi"] <= 45): score += 1
    ob_width  = ob["ob_high"] - ob["ob_low"]
    fvg_width = fvg["zone_high"] - fvg["zone_low"]
    if ob_width > 0 and fvg_width >= ob_width * 0.5:        score += 1
    rating = "PRIME" if score >= 6 else "STRONG" if score >= 5 else "GOOD" if score >= 3 else "SKIP"
    return score, rating


# =============================================================================
#  SCAN ONE SYMBOL
# =============================================================================
async def scan_symbol(symbol):
    now = datetime.now(WAT).strftime("%H:%M:%S WAT")
    h4, h1, m15, m5 = await fetch_symbol_data(symbol)
    if any(x is None for x in (h4, h1, m15, m5)):
        log.error("%s — fetch failed.", symbol); return

    h4c, h1c, m15c, m5c = closed_only(h4), closed_only(h1), closed_only(m15), closed_only(m5)

    h4_bias, h4_adx = get_h4_bias(h4c)
    ob = find_h1_order_block(h1c)

    print(f"\n  {symbol}  |  {now}", flush=True)
    print(f"  H4 Bias: {h4_bias}  |  H4 ADX: {h4_adx:.1f}", flush=True)

    if not ob["direction"]:
        print(f"  H1: No order block / structure break found", flush=True)
        return

    print(f"  H1 OB: {ob['direction']}  zone {ob['ob_low']}-{ob['ob_high']}  |  ADX {ob['adx']:.1f}", flush=True)

    if ob["direction"] == "BULL" and h4_bias != "BULLISH":
        print(f"  SKIPPED — H4 bias is {h4_bias} (needs BULLISH)", flush=True)
        return
    if ob["direction"] == "BEAR" and h4_bias != "BEARISH":
        print(f"  SKIPPED — H4 bias is {h4_bias} (needs BEARISH)", flush=True)
        return
    if ob["adx"] < CFG.adx_min:
        print(f"  SKIPPED — H1 ADX {ob['adx']:.1f} too weak (min {CFG.adx_min})", flush=True)
        return

    fvg = find_m15_fvg_nested(m15c, ob)
    if not fvg["fvg"]:
        print(f"  M15: No nested FVG found yet", flush=True)
        return

    print(f"  M15 FVG nested: zone {fvg['zone_low']}-{fvg['zone_high']}", flush=True)

    if not is_duplicate(symbol, ob["direction"] + "_ZONE", fvg["zone_low"], fvg["zone_high"]):
        send_telegram(build_zone_headsup(symbol, ob["direction"], ob, fvg, h4_bias, now))
        mark_sent(symbol, ob["direction"] + "_ZONE", fvg["zone_low"], fvg["zone_high"])

    trigger = find_m5_trigger(m5c, ob, fvg)

    if trigger.get("expired"):
        print(f"  M5: Zone EXPIRED — no trigger within {CFG.zone_max_age_h1_bars}h", flush=True)
        return

    if not trigger["trigger"]:
        print(f"  M5: Watching zone {fvg['zone_low']}-{fvg['zone_high']}...", flush=True)
        return

    score, rating = score_signal(ob, fvg, trigger)
    trade = build_trade(ob, fvg, trigger, score)
    if not trade:
        return

    print(f"  M5 Trigger: CONFIRMED  wick={trigger['wick_ratio']}  RSI={trigger['rsi']}", flush=True)
    print(f"  Entry: {trade['entry']}  SL: {trade['sl']}  RR: 1:{trade['rr']}", flush=True)
    print(f"  TP1: {trade['tp1']}  TP2: {trade['tp2']}", flush=True)

    if not is_duplicate(symbol, ob["direction"] + "_ENTRY", fvg["zone_low"], fvg["zone_high"]):
        msg = build_alert(
            symbol=symbol, direction=ob["direction"],
            zone_low=fvg["zone_low"], zone_high=fvg["zone_high"],
            entry=trade["entry"], sl=trade["sl"], tp1=trade["tp1"], tp2=trade["tp2"],
            risk=trade["risk"], rr=trade["rr"],
            ob_bar=ob["ob_bar"], fvg_bar=fvg["fvg_bar"], m5_bar=trigger["trigger_bar"],
            wick_ratio=trigger["wick_ratio"], rsi_val=trigger["rsi"],
            score=score, rating=rating, h4_bias=h4_bias,
        )
        send_telegram(msg)
        mark_sent(symbol, ob["direction"] + "_ENTRY", fvg["zone_low"], fvg["zone_high"])
    else:
        print(f"  Telegram: SKIPPED (duplicate entry)", flush=True)


# =============================================================================
#  MAIN
# =============================================================================
async def scan_all():
    now = datetime.now(WAT).strftime("%H:%M:%S WAT")
    print(f"\nSMC OB+FVG Scanner | {now} | {', '.join(CFG.symbols)}", flush=True)
    for symbol in CFG.symbols:
        try:
            await scan_symbol(symbol)
            await asyncio.sleep(2)
        except Exception as e:
            log.error("Error scanning %s: %s", symbol, e)


async def main():
    print("Deriv SMC OB+FVG Scanner | H4 Bias + H1 OB + M15 FVG + M5 Trigger", flush=True)
    if CFG.live_mode:
        try:
            while True:
                await scan_all()
                await asyncio.sleep(300)
        except KeyboardInterrupt:
            print("Stopped.")
    else:
        await scan_all()

if __name__ == "__main__":
    asyncio.run(main())
