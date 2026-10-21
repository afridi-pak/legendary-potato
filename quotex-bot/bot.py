#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quotex Signal Bot v17
- Strategy registry (easy to extend)
- New strategies: MACD+RSI confluence, MACD+BB breakout,
  MACD divergence, MACD histogram scalping
- Data quality tagging (no synthetic candles)
- Structured logging with rotation
- Signal journal (data/signals.jsonl)
"""

import sys, os, json, time, math, random, threading, logging
import urllib.request, urllib.parse, ssl
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime

if sys.version_info < (3, 8):
    print("Python 3.8+ required"); sys.exit(1)

for d in ["data", "logs"]:
    Path(d).mkdir(exist_ok=True)

# ============================================================
#   LOGGING
# ============================================================
LOG_FILE = Path("logs/bot.log")
logger = logging.getLogger("quotex")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
_fh = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
logger.addHandler(_fh)
logger.addHandler(_sh)

def log(msg, level="info"):
    getattr(logger, level if level in ("info","warning","error","debug") else "info")(msg)

# ============================================================
#   CONFIG
# ============================================================
BOT_TOKEN = "8733882861:AAEpvcBAxJWtnfHWBTju8mOZCaNBTTtw4i8"
_OWNER = 6919881622

SYMBOLS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
    "USDCHF", "GBPCAD", "GBPCHF", "CHFJPY",
    "EURAUD", "AUDCAD", "CADJPY", "GBPJPY", "EURGBP",
    "NZDUSD", "EURJPY", "AUDJPY", "EURNZD",
]

YF_MAP = {s: s + "=X" for s in SYMBOLS}

SCAN_INTERVAL = 300
MIN_CONFIDENCE = 55
COUNTDOWN_SECONDS = 5
DEDUP_SECONDS = 300

CACHE_TTL = 30
MAX_CACHE = 40

USERS_FILE = Path("data/users.json")
AUTO_FLAG = Path("data/auto.flag")
SIGNAL_JOURNAL = Path("data/signals.jsonl")

TG = f"https://api.telegram.org/bot{BOT_TOKEN}"

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

_CANDLE_CACHE = {}
_LAST_SIGNAL_MEM = {}
_SIGNAL_STATS = {"CALL": 0, "PUT": 0}

# ============================================================
#   HTTP
# ============================================================
def http_get(url, params=None, timeout=10, headers=None):
    try:
        if params:
            url = url + "?" + urllib.parse.urlencode(params)
        h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        if headers: h.update(headers)
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        log(f"http_get fail: {e}", "debug")
        return None

def http_get_text(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
            return r.read().decode("utf-8", errors="ignore")
    except Exception:
        return None

def http_post(url, data, timeout=10):
    try:
        body = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={
            "Content-Type": "application/json", "User-Agent": "Mozilla/5.0"
        })
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None

# ============================================================
#   DATA SOURCES (no synthetic data)
# ============================================================
def fetch_yahoo(symbol, interval="1m", limit=60):
    yf = YF_MAP.get(symbol)
    if not yf: return None
    ranges = ["1d", "5d", "1mo"] if interval in ("1m","2m","5m") else ["5d","1mo"]
    for rng in ranges:
        for _ in range(2):
            data = http_get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{yf}",
                {"interval": interval, "range": rng}, timeout=10)
            if not data:
                time.sleep(0.3); continue
            try:
                r0 = data["chart"]["result"][0]
                ts = r0.get("timestamp", [])
                q = r0["indicators"]["quote"][0]
                o, h, l, c = q.get("open",[]), q.get("high",[]), q.get("low",[]), q.get("close",[])
                out = []
                for i in range(len(ts)):
                    if i >= len(o) or None in (o[i],h[i],l[i],c[i]): continue
                    out.append({"t": ts[i], "open": float(o[i]), "high": float(h[i]),
                                "low": float(l[i]), "close": float(c[i])})
                if len(out) >= 20:
                    if interval == "1m":
                        cutoff = time.time() - 7200
                        recent = [x for x in out if x["t"] >= cutoff]
                        if len(recent) >= 20: return recent[-limit:]
                    return out[-limit:]
            except Exception:
                time.sleep(0.2); continue
    return None

def fetch_stooq(symbol, limit=60):
    """Stooq daily CSV — fallback only, tagged as daily quality."""
    url = f"https://stooq.com/q/d/l/?s={symbol.lower()}&i=d"
    text = http_get_text(url, timeout=8)
    if not text: return None
    lines = text.strip().split("\n")
    if len(lines) < 20: return None
    out = []
    for line in lines[1:]:
        p = line.split(",")
        if len(p) < 5: continue
        try:
            out.append({"t": int(time.time()), "open": float(p[1]), "high": float(p[2]),
                        "low": float(p[3]), "close": float(p[4])})
        except Exception:
            continue
    return out[-limit:] if len(out) >= 20 else None

def fetch_live_price(symbol):
    yf = YF_MAP.get(symbol)
    if not yf: return None
    data = http_get(f"https://query1.finance.yahoo.com/v8/finance/chart/{yf}",
                    {"interval":"1m","range":"1d"}, timeout=6)
    if not data: return None
    try:
        meta = data["chart"]["result"][0].get("meta", {})
        price = meta.get("regularMarketPrice")
        return float(price) if price else None
    except Exception:
        return None

def fetch_candles(symbol, interval="1m", limit=60):
    """Returns (candles, source, quality) or (None, None, None)."""
    key = f"{symbol}_{interval}_{limit}"
    now = time.time()
    if key in _CANDLE_CACHE:
        data, ts, src, qual = _CANDLE_CACHE[key]
        if now - ts < CACHE_TTL and data:
            return data, src, qual
    sources = [
        ("yahoo", lambda: fetch_yahoo(symbol, interval, limit), "live"),
        ("stooq", lambda: fetch_stooq(symbol, limit) if interval == "1m" else None, "daily"),
    ]
    for name, fn, qual in sources:
        try:
            r = fn()
            if r and len(r) >= 20:
                _CANDLE_CACHE[key] = (r, now, name, qual)
                if len(_CANDLE_CACHE) > MAX_CACHE:
                    old = sorted(_CANDLE_CACHE.items(), key=lambda x: x[1][1])[:MAX_CACHE//2]
                    for k,_ in old: _CANDLE_CACHE.pop(k, None)
                log(f"{symbol}: {len(r)} candles from {name} ({qual})", "debug")
                return r, name, qual
        except Exception as e:
            log(f"{symbol} source {name} fail: {e}", "debug")
    return None, None, None

# ============================================================
#   MATH
# ============================================================
def mean(a): return sum(a)/len(a) if a else 0
def std(a):
    if len(a) < 2: return 0
    m = mean(a); return math.sqrt(sum((x-m)**2 for x in a)/len(a))
def rsi(px, p=14):
    if len(px) < p+1: return 50
    g=l=0
    for i in range(1, p+1):
        d = px[i]-px[i-1]
        if d>0: g+=d
        else: l+=-d
    if l==0: return 100
    return 100 - 100/(1 + (g/p)/(l/p))
def ema_series(px, p):
    if not px or len(px) < p: return []
    k = 2/(p+1); out=[px[0]]
    for x in px[1:]: out.append(x*k + out[-1]*(1-k))
    return out
def ema(px, p):
    s = ema_series(px, p)
    return s[-1] if s else 0
def macd_series(px):
    if len(px) < 26: return [], []
    e12 = ema_series(px, 12); e26 = ema_series(px, 26)
    macd = [a-b for a,b in zip(e12, e26)]
    signal = ema_series(macd, 9)
    return macd, signal
def atr(candles, p=14):
    if len(candles) < p+1: return 0
    trs=[]
    for i in range(1, len(candles)):
        h,l,pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    return mean(trs[-p:])
def momentum(px, p=5):
    if len(px) < p+1: return 0
    return (px[-1]-px[-p-1])/px[-p-1]*100
def stoch(candles, p=14):
    if len(candles) < p: return 50
    r = candles[-p:]
    hi = max(c["high"] for c in r); lo = min(c["low"] for c in r)
    if hi==lo: return 50
    return (candles[-1]["close"]-lo)/(hi-lo)*100
def adx(candles, p=14):
    if len(candles) < p+1: return 0
    pdm, mdm, trs = [], [], []
    for i in range(1, len(candles)):
        up = candles[i]["high"]-candles[i-1]["high"]
        dn = candles[i-1]["low"]-candles[i]["low"]
        pdm.append(up if up>dn and up>0 else 0)
        mdm.append(dn if dn>up and dn>0 else 0)
        h,l,pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    if not trs: return 0
    a = mean(trs[-p:])
    if a==0: return 0
    pdi = 100*mean(pdm[-p:])/a; mdi = 100*mean(mdm[-p:])/a
    if pdi+mdi==0: return 0
    return 100*abs(pdi-mdi)/(pdi+mdi)

# ============================================================
#   PATTERNS
# ============================================================
def bull_engulf(a,b):
    return a["close"]>a["open"] and b["close"]<b["open"] and a["close"]>b["open"] and a["open"]<b["close"]
def bear_engulf(a,b):
    return a["close"]<a["open"] and b["close"]>b["open"] and a["close"]<b["open"] and a["open"]>b["close"]
def bull_pinbar(c):
    rng = c["high"]-c["low"]
    if rng==0: return False
    body = abs(c["close"]-c["open"])
    lower = min(c["open"],c["close"])-c["low"]
    return lower>body*2 and body/rng<0.35
def bear_pinbar(c):
    rng = c["high"]-c["low"]
    if rng==0: return False
    body = abs(c["close"]-c["open"])
    upper = c["high"]-max(c["open"],c["close"])
    return upper>body*2 and body/rng<0.35

# ============================================================
#   STRATEGIES  (registry)
#   Each: fn(candles, closes) -> dict(direction, score, reason) or None
# ============================================================
def _st_rsi(candles, closes):
    r = rsi(closes, 14)
    if r < 25: return {"direction":"CALL","score":22,"reason":f"RSI {r:.0f} OS"}
    if r < 35: return {"direction":"CALL","score":14,"reason":f"RSI {r:.0f} wOS"}
    if r > 75: return {"direction":"PUT","score":22,"reason":f"RSI {r:.0f} OB"}
    if r > 65: return {"direction":"PUT","score":14,"reason":f"RSI {r:.0f} wOB"}
    return None

def _st_stoch(candles, closes):
    s = stoch(candles, 14)
    if s < 20: return {"direction":"CALL","score":12,"reason":f"Stoch {s:.0f} OS"}
    if s > 80: return {"direction":"PUT","score":12,"reason":f"Stoch {s:.0f} OB"}
    return None

def _st_macd_cross(candles, closes):
    if len(closes) < 27: return None
    m, sig = macd_series(closes)
    if len(m) < 2 or len(sig) < 2: return None
    hist, hist_p = m[-1]-sig[-1], m[-2]-sig[-2]
    if hist_p < 0 and hist > 0: return {"direction":"CALL","score":18,"reason":"MACD bull X"}
    if hist_p > 0 and hist < 0: return {"direction":"PUT","score":18,"reason":"MACD bear X"}
    if m[-1] > m[-2] and m[-1] > 0: return {"direction":"CALL","score":7,"reason":"MACD up"}
    if m[-1] < m[-2] and m[-1] < 0: return {"direction":"PUT","score":7,"reason":"MACD down"}
    return None

def _st_bb(candles, closes):
    r20 = closes[-20:]; ma = mean(r20); sd = std(r20)
    if sd == 0: return None
    if closes[-1] <= ma - 2*sd: return {"direction":"CALL","score":15,"reason":"Lower BB"}
    if closes[-1] >= ma + 2*sd: return {"direction":"PUT","score":15,"reason":"Upper BB"}
    if closes[-1] <= ma - 1.2*sd: return {"direction":"CALL","score":7,"reason":"Near lower BB"}
    if closes[-1] >= ma + 1.2*sd: return {"direction":"PUT","score":7,"reason":"Near upper BB"}
    return None

def _st_ema_cross(candles, closes):
    if len(closes) < 22: return None
    e9, e21 = ema(closes,9), ema(closes,21)
    e9p, e21p = ema(closes[:-1],9), ema(closes[:-1],21)
    if e9 > e21 and e9p <= e21p: return {"direction":"CALL","score":15,"reason":"EMA bull X"}
    if e9 < e21 and e9p >= e21p: return {"direction":"PUT","score":15,"reason":"EMA bear X"}
    if e9 > e21: return {"direction":"CALL","score":6,"reason":"EMA up"}
    if e9 < e21: return {"direction":"PUT","score":6,"reason":"EMA down"}
    return None

def _st_pattern(candles, closes):
    if len(candles) < 2: return None
    last, prev = candles[-1], candles[-2]
    if bull_engulf(last, prev): return {"direction":"CALL","score":15,"reason":"Bull engulf"}
    if bear_engulf(last, prev): return {"direction":"PUT","score":15,"reason":"Bear engulf"}
    if bull_pinbar(last): return {"direction":"CALL","score":12,"reason":"Bull pinbar"}
    if bear_pinbar(last): return {"direction":"PUT","score":12,"reason":"Bear pinbar"}
    return None

def _st_momentum(candles, closes):
    m = momentum(closes, 5)
    if m > 0.15: return {"direction":"CALL","score":10,"reason":f"Mom +{m:.2f}%"}
    if m < -0.15: return {"direction":"PUT","score":10,"reason":f"Mom {m:.2f}%"}
    return None

# --- NEW: MACD + RSI confluence ---
def _st_macd_rsi_confluence(candles, closes):
    if len(closes) < 30: return None
    r = rsi(closes, 14)
    m, sig = macd_series(closes)
    if len(m) < 2 or len(sig) < 2: return None
    hist, hist_p = m[-1]-sig[-1], m[-2]-sig[-2]
    if hist_p < 0 and hist > 0 and r > 50:
        return {"direction":"CALL","score":15,"reason":"MACD X + RSI>50"}
    if hist_p > 0 and hist < 0 and r < 50:
        return {"direction":"PUT","score":15,"reason":"MACD X + RSI<50"}
    return None

# --- NEW: MACD + Bollinger breakout ---
def _st_macd_bb_breakout(candles, closes):
    if len(closes) < 30: return None
    r20 = closes[-20:]; ma = mean(r20); sd = std(r20)
    if sd == 0: return None
    m, sig = macd_series(closes)
    if len(m) < 2 or len(sig) < 2: return None
    hist, hist_p = m[-1]-sig[-1], m[-2]-sig[-2]
    up_break = closes[-1] >= ma + 2*sd
    dn_break = closes[-1] <= ma - 2*sd
    if up_break and hist_p < 0 and hist > 0:
        return {"direction":"CALL","score":18,"reason":"BB↑ + MACD bull X"}
    if dn_break and hist_p > 0 and hist < 0:
        return {"direction":"PUT","score":18,"reason":"BB↓ + MACD bear X"}
    return None

# --- NEW: MACD divergence (bullish / bearish) ---
def _find_swings(px, k=3):
    """Return (low_idxs, high_idxs) local extremes with window k."""
    lows, highs = [], []
    for i in range(k, len(px)-k):
        w = px[i-k:i+k+1]
        if px[i] == min(w): lows.append(i)
        if px[i] == max(w): highs.append(i)
    return lows, highs

def _st_macd_divergence(candles, closes):
    if len(closes) < 30: return None
    m, sig = macd_series(closes)
    if len(m) < 30: return None
    lows, highs = _find_swings(closes, 3)
    # bullish: price lower low, macd higher low
    if len(lows) >= 2:
        a, b = lows[-2], lows[-1]
        if b - a >= 3 and closes[b] < closes[a] and m[b] > m[a]:
            return {"direction":"CALL","score":18,"reason":"Bull divergence"}
    if len(highs) >= 2:
        a, b = highs[-2], highs[-1]
        if b - a >= 3 and closes[b] > closes[a] and m[b] < m[a]:
            return {"direction":"PUT","score":18,"reason":"Bear divergence"}
    return None

# --- NEW: MACD histogram scalping ---
def _st_macd_hist_scalp(candles, closes):
    if len(closes) < 30: return None
    m, sig = macd_series(closes)
    if len(m) < 3 or len(sig) < 3: return None
    h1 = m[-1]-sig[-1]; h2 = m[-2]-sig[-2]; h3 = m[-3]-sig[-3]
    if h2 < 0 and h1 > 0 and h1 > h2:
        return {"direction":"CALL","score":13,"reason":"Hist flip +"}
    if h2 > 0 and h1 < 0 and h1 < h2:
        return {"direction":"PUT","score":13,"reason":"Hist flip -"}
    # shrinking histogram in trend direction = weakening — no strong signal
    if abs(h1) < abs(h2) < abs(h3) and h1 > 0:
        return {"direction":"PUT","score":5,"reason":"Hist fading up"}
    if abs(h1) < abs(h2) < abs(h3) and h1 < 0:
        return {"direction":"CALL","score":5,"reason":"Hist fading down"}
    return None

STRATEGIES = [
    {"name":"rsi",          "weight":1.0, "fn":_st_rsi},
    {"name":"stoch",        "weight":1.0, "fn":_st_stoch},
    {"name":"macd_cross",   "weight":1.0, "fn":_st_macd_cross},
    {"name":"bb",           "weight":1.0, "fn":_st_bb},
    {"name":"ema_cross",    "weight":1.0, "fn":_st_ema_cross},
    {"name":"pattern",      "weight":1.0, "fn":_st_pattern},
    {"name":"momentum",     "weight":1.0, "fn":_st_momentum},
    {"name":"macd_rsi",     "weight":1.0, "fn":_st_macd_rsi_confluence},
    {"name":"macd_bb",      "weight":1.0, "fn":_st_macd_bb_breakout},
    {"name":"macd_div",     "weight":1.0, "fn":_st_macd_divergence},
    {"name":"macd_hist",    "weight":1.0, "fn":_st_macd_hist_scalp},
]

def run_strategies(candles):
    """Return (call, put, reasons[], details{})."""
    closes = [c["close"] for c in candles]
    call = put = 0
    reasons = []
    details = {}
    for s in STRATEGIES:
        try:
            r = s["fn"](candles, closes)
        except Exception as e:
            log(f"strategy {s['name']} err: {e}", "warning")
            continue
        if not r: continue
        w = s["weight"]
        pts = int(r["score"] * w)
        if r["direction"] == "CALL": call += pts
        else: put += pts
        reasons.append(r["reason"])
    # Diagnostics
    details["rsi"] = round(rsi(closes,14), 1)
    details["stoch"] = round(stoch(candles,14), 1)
    details["adx"] = round(adx(candles,14), 1)
    details["mom"] = round(momentum(closes,5), 3)
    m, sig = macd_series(closes)
    details["macd"] = round((m[-1]-sig[-1]) if m and sig else 0, 6)
    return call, put, reasons, details

# ============================================================
#   ANALYSIS
# ============================================================
def suggest_duration(conf, atr_val, price, adx_val):
    if price == 0: return "2 minutes", "Default"
    vol_pct = atr_val / price * 100
    if conf >= 80 and adx_val >= 25: return "5 minutes", "Strong trend"
    if conf >= 75 and vol_pct > 0.05: return "3 minutes", "High volatility"
    if conf >= 70: return "3 minutes", "Good setup"
    if conf >= 60: return "2 minutes", "Balanced"
    return "1 minute", "Quick scalp"

def analyze(symbol):
    candles, source, quality = fetch_candles(symbol, "1m", 60)
    if not candles or len(candles) < 30:
        return None
    live = fetch_live_price(symbol)
    price = live if live else candles[-1]["close"]

    call, put, reasons, details = run_strategies(candles)
    total = call + put

    if total == 0:
        return None

    if call > put + 5: direction = "CALL"
    elif put > call + 5: direction = "PUT"
    else:
        last3 = candles[-3:]
        up = sum(1 for c in last3 if c["close"] > c["open"])
        direction = "CALL" if up >= 2 else "PUT"

    winner = call if direction == "CALL" else put
    conf = min(95, int((winner / total) * 100) + 10)

    # ADX filter: chop reduces confidence
    if details["adx"] < 20:
        conf = max(MIN_CONFIDENCE, conf - 10)
    # Very few agreeing strategies = weak
    if len(reasons) < 2:
        conf = max(MIN_CONFIDENCE, conf - 5)

    atr_val = atr(candles, 14)
    dur, dur_r = suggest_duration(conf, atr_val, price, details["adx"])
    sig_list = list(dict.fromkeys(reasons))[:6] or ["Mixed"]

    _SIGNAL_STATS[direction] = _SIGNAL_STATS.get(direction, 0) + 1

    result = {
        "direction": direction, "confidence": conf, "signals": sig_list,
        "price": price, "duration": dur, "dur_reason": dur_r,
        "details": details, "atr": atr_val, "symbol": symbol,
        "source": source, "quality": quality,
        "ts": datetime.utcnow().isoformat() + "Z",
    }
    _journal_signal(result)
    return result

def _journal_signal(sig):
    try:
        with SIGNAL_JOURNAL.open("a", encoding="utf-8") as f:
            f.write(json.dumps(sig, separators=(",", ":")) + "\n")
    except Exception as e:
        log(f"journal fail: {e}", "warning")

# ============================================================
#   SCAN / DEDUP
# ============================================================
def scan_auto():
    pool = SYMBOLS.copy(); random.shuffle(pool)
    best = None
    for sym in pool[:8]:
        try:
            r = analyze(sym)
            if not r: continue
            if is_duplicate(r): continue
            if best is None or r["confidence"] > best["confidence"]:
                best = r
        except Exception as e:
            log(f"scan {sym} fail: {e}", "debug")
    if best: mark_signal(best)
    return best

def is_duplicate(sig):
    prev = _LAST_SIGNAL_MEM.get(sig["symbol"], {})
    if prev.get("direction") == sig["direction"]:
        if time.time() - prev.get("time",0) < DEDUP_SECONDS:
            return True
    return False

def mark_signal(sig):
    _LAST_SIGNAL_MEM[sig["symbol"]] = {"direction": sig["direction"], "time": time.time()}

# ============================================================
#   USERS
# ============================================================
def load_users():
    try:
        if USERS_FILE.exists():
            c = USERS_FILE.read_text(encoding="utf-8").strip()
            if not c: return {"users": {}}
            d = json.loads(c)
            if not isinstance(d, dict): return {"users": {}}
            d.setdefault("users", {})
            return d
    except Exception: pass
    return {"users": {}}

def save_users(d):
    try:
        d.setdefault("users", {})
        USERS_FILE.write_text(json.dumps(d, separators=(",",":")), encoding="utf-8")
    except Exception: pass

def _is_owner(uid):
    try: return int(uid) == int(_OWNER)
    except Exception: return False

def get_role(uid):
    if _is_owner(uid): return "owner"
    u = load_users().get("users", {}).get(str(uid), {})
    if not u.get("active", True): return "banned"
    if u.get("role") == "vip": return "vip"
    return "normal"

def register(uid, name=""):
    d = load_users(); k = str(uid)
    if k not in d["users"]:
        d["users"][k] = {"id": int(uid), "name": name, "role": "normal",
                         "active": True, "signals": 0}
        save_users(d)
    elif name and d["users"][k].get("name") != name:
        d["users"][k]["name"] = name; save_users(d)

def approve(uid):
    d = load_users(); k = str(uid)
    d["users"].setdefault(k, {"id": int(uid), "name": "", "role": "vip",
                              "active": True, "signals": 0})
    d["users"][k]["role"] = "vip"; d["users"][k]["active"] = True
    save_users(d)

def revoke(uid):
    d = load_users()
    if str(uid) in d["users"]:
        d["users"][str(uid)]["role"] = "normal"; save_users(d)

def ban(uid, state=True):
    d = load_users()
    if str(uid) in d["users"]:
        d["users"][str(uid)]["active"] = not state; save_users(d)

def inc_signals(uid):
    d = load_users(); k = str(uid)
    if k in d["users"]:
        d["users"][k]["signals"] = d["users"][k].get("signals", 0) + 1
        save_users(d)

def all_users(): return load_users().get("users", {})
def approved_users():
    return [u for u in all_users().values() if u.get("role")=="vip" and u.get("active", True)]

# ============================================================
#   TELEGRAM
# ============================================================
def tg_send(chat_id, text, reply_markup=None):
    p = {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
         "disable_web_page_preview": True}
    if reply_markup: p["reply_markup"] = reply_markup
    return http_post(f"{TG}/sendMessage", p, timeout=10)

def tg_edit(chat_id, message_id, text, reply_markup=None):
    p = {"chat_id": chat_id, "message_id": message_id, "text": text,
         "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup: p["reply_markup"] = reply_markup
    return http_post(f"{TG}/editMessageText", p, timeout=10)

def tg_answer(cb_id, text=""):
    return http_post(f"{TG}/answerCallbackQuery",
                     {"callback_query_id": cb_id, "text": text}, timeout=5)

def tg_poll(offset):
    r = http_get(f"{TG}/getUpdates", {"timeout": 10, "offset": offset or 0}, timeout=15)
    return r.get("result", []) if r else []

# ============================================================
#   UI
# ============================================================
def build_market_keyboard():
    btns = []; row = []
    for sym in SYMBOLS:
        row.append({"text": f"📈 {sym}", "callback_data": f"mkt:{sym}"})
        if len(row) == 2: btns.append(row); row = []
    if row: btns.append(row)
    btns.append([{"text":"🎲 Random", "callback_data":"mkt:RANDOM"},
                 {"text":"📊 Stats", "callback_data":"stats:1"}])
    return {"inline_keyboard": btns}

def build_start_text(name, role):
    txt = (
        "<b>🤖 Quotex Signal Bot v17</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {name or 'User'}\n"
        f"📊 {len(SYMBOLS)} markets | {len(STRATEGIES)} strategies\n"
        f"⏱ Scan: {SCAN_INTERVAL//60} min\n"
        "━━━━━━━━━━━━━━━━━━━\n\n"
        "👇 <b>Select a market:</b>\n\n"
        "/start /markets /signal /test /myid /help"
    )
    if role in ("vip","owner"): txt += "\n/autostart /stop /stats /journal"
    return txt

def build_countdown_text(symbol, seconds):
    if seconds > 0:
        bt=5; f=bt-seconds
        bar = "█"*f + "░"*(bt-f)
        return (f"⏳ <b>Analyzing {symbol}...</b>\n"
                "━━━━━━━━━━━━━━━━━━━\n"
                f"<code>{bar}</code> <b>{seconds}s</b>\n"
                "━━━━━━━━━━━━━━━━━━━\n"
                "🔍 11 strategies running")
    return f"✅ <b>{symbol}</b> — Signal ready!"

def fmt_signal(sig, uid):
    role = get_role(uid)
    icon = "🟢" if sig["direction"]=="CALL" else "🔴"
    d = "BUY ⬆️" if sig["direction"]=="CALL" else "SELL ⬇️"
    size = "1.5-2%" if sig["confidence"]>=85 else ("1%" if sig["confidence"]>=70 else "0.5%")
    at = "\n".join(f"  • {s}" for s in sig["signals"][:6])
    src = sig.get("source","?"); qual = sig.get("quality","?")
    txt = (
        f"🎯 Asset: {sig['symbol']}\n"
        f"📊 Direction: {d}\n"
        f"⏱ Duration: {sig['duration']}\n"
        f"🎚 Confidence: {sig['confidence']}%\n"
        f"💵 Entry: {sig['price']:.5f}\n"
        f"💰 Size: {size}\n"
        f"📡 Source: {src} ({qual})\n"
        f"\n📝 Analysis:\n{at}"
    )
    extra = ""
    if role in ("vip","owner"):
        d1 = sig.get("details", {})
        extra = ("\n<b>DEEP DETAIL:</b>\n"
                 f"  RSI={d1.get('rsi','-')}  Stoch={d1.get('stoch','-')}\n"
                 f"  MACD={d1.get('macd','-')}  ADX={d1.get('adx','-')}\n"
                 f"  Mom={d1.get('mom','-')}%  ATR={sig.get('atr',0):.5f}\n")
    return (f"{icon} <b>QUOTEX SIGNAL</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            f"<code>{txt}</code>\n{extra}"
            "━━━━━━━━━━━━━━━━━━━\n"
            f"💡 <i>{sig.get('dur_reason','')}</i>\n"
            "⚠️ Risk 1-2% only")

def send_signal_with_countdown(chat_id, sig):
    symbol = sig.get("symbol","?")
    resp = tg_send(chat_id, build_countdown_text(symbol, COUNTDOWN_SECONDS))
    if not resp or not resp.get("ok"):
        tg_send(chat_id, fmt_signal(sig, chat_id)); inc_signals(chat_id); return
    mid = resp["result"]["message_id"]
    for sec in range(COUNTDOWN_SECONDS-1, 0, -1):
        time.sleep(1)
        try: tg_edit(chat_id, mid, build_countdown_text(symbol, sec))
        except Exception: pass
    time.sleep(0.4)
    try: tg_edit(chat_id, mid, fmt_signal(sig, chat_id))
    except Exception: tg_send(chat_id, fmt_signal(sig, chat_id))
    inc_signals(chat_id)

# ============================================================
#   CALLBACK
# ============================================================
def handle_callback(cb):
    try:
        chat = cb["message"]["chat"]["id"]; user = cb["from"]["id"]
        msg_id = cb["message"]["message_id"]; cb_id = cb["id"]
        data = cb.get("data","")
        register(user, cb["from"].get("first_name",""))
        role = get_role(user)
        if role == "banned": tg_answer(cb_id, "Access denied"); return
        if role == "normal" and not _is_owner(user):
            tg_answer(cb_id, "Not approved"); return
        if data.startswith("stats:"):
            tg_answer(cb_id, f"CALL:{_SIGNAL_STATS.get('CALL',0)} PUT:{_SIGNAL_STATS.get('PUT',0)}")
            return
        if not data.startswith("mkt:"):
            tg_answer(cb_id, ""); return
        target = data.split(":",1)[1]
        if target == "RANDOM":
            target = random.choice(SYMBOLS); tg_answer(cb_id, f"🎲 {target}")
        else:
            tg_answer(cb_id, f"Analyzing {target}...")
        tg_edit(chat, msg_id,
                f"⏳ <b>Analyzing {target}...</b>\n"
                "━━━━━━━━━━━━━━━━━━━\n"
                "📊 Fetching live data...\n"
                "🔄 Yahoo primary, Stooq fallback")
        sig = analyze(target)
        if not sig:
            _CANDLE_CACHE.pop(f"{target}_1m_60", None)
            time.sleep(1); sig = analyze(target)
        if not sig:
            tg_edit(chat, msg_id,
                f"⚠️ <b>{target}</b> — data unavailable\n"
                "━━━━━━━━━━━━━━━━━━━\n"
                "• Market closed (weekend)\n"
                "• API rate limit\n"
                "• Network timeout\n\n"
                "🔄 Try another market or /signal")
            tg_send(chat, "👇 Pick another:", reply_markup=build_market_keyboard())
            return
        mark_signal(sig)
        for sec in range(COUNTDOWN_SECONDS-1, 0, -1):
            try: tg_edit(chat, msg_id, build_countdown_text(target, sec))
            except Exception: pass
            time.sleep(1)
        time.sleep(0.4)
        try: tg_edit(chat, msg_id, fmt_signal(sig, chat))
        except Exception: tg_send(chat, fmt_signal(sig, chat))
        inc_signals(user)
        tg_send(chat, "🔄 <b>Pick another:</b>", reply_markup=build_market_keyboard())
    except Exception as e:
        log(f"callback err: {e}", "error")

# ============================================================
#   HANDLE
# ============================================================
def handle(msg):
    try:
        chat = msg["chat"]["id"]; user = msg["from"]["id"]
        name = msg["from"].get("first_name",""); text = msg.get("text","").strip()
        log(f"msg {user} ({name}): {text[:50]}")
        register(user, name); role = get_role(user)
        if role == "banned": tg_send(chat, "🚫 Access denied."); return
        if role == "normal" and not _is_owner(user):
            tg_send(chat, "🔒 <b>Access Required</b>\n"
                          "━━━━━━━━━━━━━━━━━━━\n\n"
                          "Send this ID to admin:\n"
                          f"🆔 <code>{user}</code>")
            return
        if text.startswith("/start"):
            tg_send(chat, build_start_text(name, role), reply_markup=build_market_keyboard()); return
        if text.startswith("/markets"):
            tg_send(chat, "👇 <b>Select a market:</b>", reply_markup=build_market_keyboard()); return
        if text.startswith("/help"):
            b = "/start /markets /signal /test /myid /help"
            if role in ("vip","owner"): b += "\n/autostart /stop /stats /journal"
            tg_send(chat, b); return
        if text.startswith("/myid"):
            tg_send(chat, f"🆔 <code>{user}</code>"); return
        if text.startswith("/signal"):
            tg_send(chat, "🔍 Auto-scanning best market...")
            s = scan_auto()
            if s:
                send_signal_with_countdown(chat, s)
                tg_send(chat, "🔄 Pick another:", reply_markup=build_market_keyboard())
            else:
                tg_send(chat, "⚠️ No data. Try again in 10 sec.")
            return
        if text.startswith("/test"):
            tg_send(chat, "Testing sources...")
            lines = []
            for s in SYMBOLS[:5]:
                c, src, q = fetch_candles(s, "1m", 20)
                lines.append(f"{'✅' if c else '❌'} {s} ({len(c) if c else 0}, {src or '-'})")
            tg_send(chat, "Active test:\n" + "\n".join(lines)); return
        if text.startswith("/autostart"):
            if role not in ("vip","owner"): tg_send(chat, "🔒 VIP only"); return
            AUTO_FLAG.write_text(str(int(time.time())))
            tg_send(chat, f"▶️ Auto ({SCAN_INTERVAL//60} min)"); return
        if text.startswith("/stop"):
            if role not in ("vip","owner"): return
            try: AUTO_FLAG.unlink()
            except Exception: pass
            tg_send(chat, "⏹ Stopped"); return
        if text.startswith("/stats"):
            if role not in ("vip","owner"): return
            users = all_users()
            total = len(users)
            ap = len([u for u in users.values() if u.get("role")=="vip"])
            tg_send(chat, f"Total: {total}\nApproved: {ap}\n"
                          f"CALL: {_SIGNAL_STATS.get('CALL',0)}\n"
                          f"PUT: {_SIGNAL_STATS.get('PUT',0)}"); return
        if text.startswith("/journal"):
            if role not in ("vip","owner"): return
            if not SIGNAL_JOURNAL.exists():
                tg_send(chat, "No signals yet."); return
            lines = SIGNAL_JOURNAL.read_text(encoding="utf-8").strip().split("\n")[-10:]
            out = []
            for ln in lines:
                try:
                    j = json.loads(ln)
                    out.append(f"{j['symbol']} {j['direction']} {j['confidence']}% ({j.get('source','?')})")
                except Exception: continue
            tg_send(chat, "Last 10 signals:\n" + "\n".join(out)); return
        if role == "owner":
            if text.startswith("/add"):
                p = text.split()
                if len(p) < 2: return
                try: t = int(p[1])
                except ValueError: return
                approve(t); tg_send(chat, f"✅ Approved {t}")
                tg_send(t, "🎉 Approved! /start"); return
            if text.startswith("/remove"):
                p = text.split()
                if len(p) < 2: return
                try: revoke(int(p[1])); tg_send(chat, "✅ Removed")
                except Exception: pass
                return
            if text.startswith("/ban"):
                p = text.split()
                if len(p) < 2: return
                try: ban(int(p[1]), True); tg_send(chat, "🚫 Banned")
                except Exception: pass
                return
            if text.startswith("/unban"):
                p = text.split()
                if len(p) < 2: return
                try: ban(int(p[1]), False); tg_send(chat, "✅ Unbanned")
                except Exception: pass
                return
            if text.startswith("/users"):
                users = all_users()
                if not users: tg_send(chat, "No users"); return
                lines = [f"<b>{len(users)} users</b>"]
                for uid, u in list(users.items())[-30:]:
                    ic = "⭐" if u.get("role")=="vip" else "👤"
                    lines.append(f"{ic} {u.get('name','?')} — <code>{uid}</code>")
                tg_send(chat, "\n".join(lines)); return
            if text.startswith("/broadcast"):
                p = text.split(maxsplit=1)
                if len(p) < 2: return
                sent = 0
                for uid in all_users().keys():
                    try: tg_send(int(uid), f"📢 {p[1]}"); sent += 1
                    except Exception: pass
                    time.sleep(0.05)
                tg_send(chat, f"✅ Sent {sent}"); return
        if text.startswith("/"):
            tg_send(chat, "❓ Use /start"); return
        tg_send(chat, "Use /start to pick a market")
    except Exception as e:
        log(f"handler err: {e}", "error")

# ============================================================
#   AUTO LOOP
# ============================================================
def auto_loop():
    last = 0
    while True:
        try:
            if AUTO_FLAG.exists() and time.time() - last >= SCAN_INTERVAL:
                log(f"Auto scan at {datetime.now().strftime('%H:%M:%S')}")
                s = scan_auto()
                if s:
                    recips = {_OWNER}
                    for u in approved_users(): recips.add(u["id"])
                    for rid in recips:
                        try: send_signal_with_countdown(rid, s)
                        except Exception as e: log(f"send {rid} err: {e}", "warning")
                    log(f"Signal {s['symbol']} {s['direction']} {s['confidence']}%")
                last = time.time()
                AUTO_FLAG.write_text(str(int(last)))
            time.sleep(5)
        except Exception as e:
            log(f"auto err: {e}", "error"); time.sleep(10)

# ============================================================
#   MAIN
# ============================================================
def main():
    log("=" * 50)
    log(f"Quotex Signal Bot v17 — {len(SYMBOLS)} markets, {len(STRATEGIES)} strategies")
    log(f"Min confidence: {MIN_CONFIDENCE}%")
    log("=" * 50)
    log("Testing data sources...")
    ok = 0
    for s in SYMBOLS[:5]:
        c, src, q = fetch_candles(s, "1m", 20)
        if c: log(f"  {s}: OK ({len(c)}, {src})"); ok += 1
        else: log(f"  {s}: FAIL", "warning")
    log(f"{ok}/5 OK")
    threading.Thread(target=auto_loop, daemon=True).start()
    offset = None
    while True:
        try:
            for up in tg_poll(offset):
                offset = up["update_id"] + 1
                if "message" in up and "text" in up["message"]:
                    handle(up["message"])
                elif "callback_query" in up:
                    handle_callback(up["callback_query"])
        except KeyboardInterrupt:
            log("Stopped"); break
        except Exception as e:
            log(f"poll err: {e}", "error"); time.sleep(3)

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt: log("Stopped")
