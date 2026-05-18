import os
import re
import time
import math
import random
import sqlite3
from datetime import datetime

import requests
from openai import OpenAI


# =========================
# 基础配置：主流币实时观察 + 搜索关键词增强版
# =========================

BINANCE_BASE_URL = "https://api.binance.com"

SYMBOLS = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "SOLUSDT": "SOL",
    "XRPUSDT": "XRP",
    "BNBUSDT": "BNB",
}

IMAGE_MAP = {
    "BTCUSDT": "btc.png",
    "ETHUSDT": "eth.png",
    "SOLUSDT": "sol.png",
    "XRPUSDT": "xrp.png",
    "BNBUSDT": "bnb.png",
}

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# 降低频率：默认 5 分钟检查一次
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))

# 降低发送频率：同一个币 30 分钟冷却，全频道 15 分钟冷却
SYMBOL_COOLDOWN_SECONDS = int(os.getenv("SYMBOL_COOLDOWN_SECONDS", "1800"))
GLOBAL_COOLDOWN_SECONDS = int(os.getenv("GLOBAL_COOLDOWN_SECONDS", "900"))

# 进一步限制行情信号数量，避免刷屏
MAX_SIGNALS_PER_DAY = int(os.getenv("MAX_SIGNALS_PER_DAY", "10"))
MAX_SIGNALS_PER_HOUR = int(os.getenv("MAX_SIGNALS_PER_HOUR", "2"))

USE_AI_POLISH = os.getenv("USE_AI_POLISH", "true").lower() == "true"
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-5.4-nano")

KLINE_LIMIT = int(os.getenv("KLINE_LIMIT", "60"))
IMAGES_DIR = os.getenv("IMAGES_DIR", "images")

# 新增：撸毛栏目
ENABLE_LM_COLUMN = os.getenv("ENABLE_LM_COLUMN", "true").lower() == "true"
LM_IMAGE = os.getenv("LM_IMAGE", "lm.png")
LM_POST_INTERVAL_SECONDS = int(os.getenv("LM_POST_INTERVAL_SECONDS", "21600"))  # 默认 6 小时
LM_FIRST_RUN_SEND = os.getenv("LM_FIRST_RUN_SEND", "true").lower() == "true"

REQUEST_TIMEOUT = 15

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


# =========================
# 阈值配置
# =========================

VOLUME_STRONG_MULTIPLIER = 1.3
VOLUME_SPIKE_MULTIPLIER = 1.8
STRONG_24H_MOVE = 1.5

FIVE_MIN_MOVE_THRESHOLD = {
    "BTCUSDT": 0.45,
    "ETHUSDT": 0.55,
    "SOLUSDT": 0.90,
    "XRPUSDT": 0.90,
    "BNBUSDT": 0.75,
}

PRICE_DECIMALS = {
    "BTCUSDT": 2,
    "ETHUSDT": 2,
    "SOLUSDT": 2,
    "XRPUSDT": 4,
    "BNBUSDT": 2,
}


# =========================
# 关键词池：每条只选 3-5 个，最多 5 个
# =========================

GENERAL_TAGS = [
    "#币圈监控", "#行情监控", "#加密货币", "#主流币", "#短线信号",
    "#行情异动", "#支撑压力", "#成交量", "#短线结构", "#市场监控",
    "#币圈行情", "#价格监控", "#趋势观察", "#资金情绪", "#多空变化",
]

EVENT_TAGS = {
    "signal_change": [
        "#信号监控", "#趋势切换", "#短线观察", "#多空变化", "#行情监控",
        "#短线信号", "#币圈行情", "#主流币监控",
    ],
    "breakout": [
        "#突破监控", "#放量突破", "#压力位", "#短线转强", "#主流币异动",
        "#行情异动", "#上涨监控", "#支撑压力",
    ],
    "breakdown": [
        "#跌破监控", "#支撑位", "#短线转弱", "#风险监控", "#行情回落",
        "#下跌监控", "#支撑压力", "#短线结构",
    ],
    "volume_spike": [
        "#放量异动", "#成交量放大", "#异动监控", "#资金异动", "#短线波动",
        "#行情异动", "#成交量", "#币圈监控",
    ],
}

SYMBOL_TAGS = {
    "BTCUSDT": ["#BTC", "#比特币", "#BTC监控", "#比特币行情", "#主流币"],
    "ETHUSDT": ["#ETH", "#以太坊", "#ETH监控", "#以太坊行情", "#主流币"],
    "SOLUSDT": ["#SOL", "#Solana", "#SOL监控", "#山寨币", "#公链生态"],
    "XRPUSDT": ["#XRP", "#XRP监控", "#山寨币", "#主流币", "#币圈行情"],
    "BNBUSDT": ["#BNB", "#BNB监控", "#币安生态", "#主流币", "#交易所生态"],
}

SIGNAL_TAGS = {
    "强偏多": ["#强偏多", "#偏多", "#短线转强"],
    "偏多": ["#偏多", "#短线偏多", "#行情观察"],
    "震荡": ["#震荡", "#观望", "#区间震荡"],
    "偏空": ["#偏空", "#短线偏空", "#风险监控"],
    "强偏空": ["#强偏空", "#偏空", "#短线转弱"],
}

LM_TAG_POOL = [
    "#撸毛", "#空投", "#空投监控", "#撸毛情报", "#测试网",
    "#测试网交互", "#积分任务", "#交互任务", "#Galxe", "#Layer3",
    "#Zealy", "#钱包交互", "#白名单", "#徽章任务", "#早期项目",
    "#空投机会", "#任务入口", "#项目官网", "#官方Discord", "#官方X",
    "#低成本交互", "#链上交互", "#空投任务", "#撸毛机会", "#任务监控",
]


# =========================
# 数据库
# =========================

def init_db():
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS symbol_state (
            symbol TEXT PRIMARY KEY,
            last_signal TEXT,
            last_support REAL,
            last_resistance REAL,
            last_event_key TEXT,
            last_sent_at REAL,
            updated_at REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT,
            symbol TEXT,
            event_type TEXT,
            created_at REAL
        )
    """)

    conn.commit()
    conn.close()


def get_state(symbol: str):
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, last_signal, last_support, last_resistance, last_event_key, last_sent_at, updated_at
        FROM symbol_state
        WHERE symbol = ?
    """, (symbol,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "symbol": row[0],
        "last_signal": row[1],
        "last_support": row[2],
        "last_resistance": row[3],
        "last_event_key": row[4],
        "last_sent_at": row[5] or 0,
        "updated_at": row[6] or 0,
    }


def upsert_state(symbol: str, signal: str, support: float, resistance: float,
                 event_key: str = None, sent_at: float = None):
    old = get_state(symbol)
    now = time.time()

    if old:
        final_event_key = event_key if event_key is not None else old.get("last_event_key")
        final_sent_at = sent_at if sent_at is not None else old.get("last_sent_at", 0)
    else:
        final_event_key = event_key
        final_sent_at = sent_at or 0

    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO symbol_state (
            symbol, last_signal, last_support, last_resistance,
            last_event_key, last_sent_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            last_signal = excluded.last_signal,
            last_support = excluded.last_support,
            last_resistance = excluded.last_resistance,
            last_event_key = excluded.last_event_key,
            last_sent_at = excluded.last_sent_at,
            updated_at = excluded.updated_at
    """, (
        symbol, signal, support, resistance,
        final_event_key, final_sent_at, now
    ))
    conn.commit()
    conn.close()


def get_meta(key: str, default=None):
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else default


def set_meta(key: str, value: str):
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (key, value))
    conn.commit()
    conn.close()


def has_any_state() -> bool:
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM symbol_state")
    count = cur.fetchone()[0]
    conn.close()
    return count > 0


def record_sent(kind: str, symbol: str = "", event_type: str = ""):
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sent_log(kind, symbol, event_type, created_at)
        VALUES (?, ?, ?, ?)
    """, (kind, symbol, event_type, time.time()))
    conn.commit()
    conn.close()


def count_sent_since(kind: str, since_ts: float) -> int:
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM sent_log
        WHERE kind = ?
          AND created_at >= ?
    """, (kind, since_ts))
    count = cur.fetchone()[0]
    conn.close()
    return count


def signal_daily_limit_ok() -> bool:
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    return count_sent_since("signal", today_start) < MAX_SIGNALS_PER_DAY


def signal_hourly_limit_ok() -> bool:
    return count_sent_since("signal", time.time() - 3600) < MAX_SIGNALS_PER_HOUR


# =========================
# Binance 数据
# =========================

def request_json(url: str, params: dict):
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_klines(symbol: str, interval: str, limit: int = 60):
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    data = request_json(url, {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    })

    candles = []
    for item in data:
        candles.append({
            "open_time": int(item[0]),
            "open": float(item[1]),
            "high": float(item[2]),
            "low": float(item[3]),
            "close": float(item[4]),
            "volume": float(item[5]),
            "close_time": int(item[6]),
        })

    return candles


def fetch_24h_ticker(symbol: str):
    url = f"{BINANCE_BASE_URL}/api/v3/ticker/24hr"
    data = request_json(url, {"symbol": symbol})
    return {
        "last_price": float(data["lastPrice"]),
        "price_change_percent": float(data["priceChangePercent"]),
        "volume": float(data["volume"]),
        "quote_volume": float(data["quoteVolume"]),
    }


# =========================
# 指标计算
# =========================

def avg(values):
    values = [v for v in values if v is not None and not math.isnan(v)]
    if not values:
        return 0
    return sum(values) / len(values)


def fmt_price(symbol: str, value: float) -> str:
    decimals = PRICE_DECIMALS.get(symbol, 2)
    return f"{value:.{decimals}f}"


def completed_candles(candles):
    if len(candles) >= 2:
        return candles[:-1]
    return candles


def is_higher_structure(candles_5):
    last5 = candles_5[-5:]
    if len(last5) < 5:
        return False

    highs = [c["high"] for c in last5]
    lows = [c["low"] for c in last5]

    return highs[-1] > highs[0] and lows[-1] > lows[0]


def is_lower_structure(candles_5):
    last5 = candles_5[-5:]
    if len(last5) < 5:
        return False

    highs = [c["high"] for c in last5]
    lows = [c["low"] for c in last5]

    return highs[-1] < highs[0] and lows[-1] < lows[0]


def analyze_symbol(symbol: str):
    candles_5_raw = fetch_klines(symbol, "5m", KLINE_LIMIT)
    candles_15_raw = fetch_klines(symbol, "15m", KLINE_LIMIT)
    ticker = fetch_24h_ticker(symbol)

    candles_5 = completed_candles(candles_5_raw)
    candles_15 = completed_candles(candles_15_raw)

    if len(candles_5) < 25 or len(candles_15) < 25:
        raise ValueError(f"{symbol} K线数量不足")

    current_price = ticker["last_price"]
    pct_24h = ticker["price_change_percent"]

    closes_5 = [c["close"] for c in candles_5]
    closes_15 = [c["close"] for c in candles_15]

    ma5_20 = avg(closes_5[-20:])
    ma15_20 = avg(closes_15[-20:])

    last20_5 = candles_5[-20:]
    support = min(c["low"] for c in last20_5)
    resistance = max(c["high"] for c in last20_5)

    last_completed_5 = candles_5[-1]
    prev5_for_volume = candles_5[-6:-1]
    avg_vol_5 = avg([c["volume"] for c in prev5_for_volume])
    current_vol_5 = last_completed_5["volume"]
    volume_ratio = current_vol_5 / avg_vol_5 if avg_vol_5 > 0 else 1

    last_5m_change_pct = (
        (last_completed_5["close"] - last_completed_5["open"]) / last_completed_5["open"] * 100
        if last_completed_5["open"] > 0 else 0
    )

    higher = is_higher_structure(candles_5)
    lower = is_lower_structure(candles_5)

    score = 0
    reasons = []

    if current_price > ma5_20:
        score += 1
        reasons.append("价格站上5分钟均线")
    else:
        score -= 1
        reasons.append("价格低于5分钟均线")

    if current_price > ma15_20:
        score += 1
        reasons.append("价格站上15分钟均线")
    else:
        score -= 1
        reasons.append("价格低于15分钟均线")

    if higher:
        score += 1
        reasons.append("近5根5分钟K线高低点抬高")

    if lower:
        score -= 1
        reasons.append("近5根5分钟K线高低点降低")

    if volume_ratio >= VOLUME_STRONG_MULTIPLIER:
        if last_5m_change_pct > 0:
            score += 1
            reasons.append("上涨伴随成交量放大")
        elif last_5m_change_pct < 0:
            score -= 1
            reasons.append("下跌伴随成交量放大")

    if pct_24h > STRONG_24H_MOVE:
        score += 1
        reasons.append("24小时涨幅偏强")
    elif pct_24h < -STRONG_24H_MOVE:
        score -= 1
        reasons.append("24小时跌幅偏深")

    if score >= 4:
        signal = "强偏多"
    elif score >= 2:
        signal = "偏多"
    elif score <= -4:
        signal = "强偏空"
    elif score <= -2:
        signal = "偏空"
    else:
        signal = "震荡"

    return {
        "symbol": symbol,
        "display": SYMBOLS[symbol],
        "price": current_price,
        "pct_24h": pct_24h,
        "ma5_20": ma5_20,
        "ma15_20": ma15_20,
        "support": support,
        "resistance": resistance,
        "volume_ratio": volume_ratio,
        "last_5m_change_pct": last_5m_change_pct,
        "signal": signal,
        "score": score,
        "reason": "，".join(reasons[:4]),
    }


# =========================
# 触发器
# =========================

def global_cooldown_ok() -> bool:
    last = float(get_meta("last_global_sent_at", "0") or 0)
    return time.time() - last >= GLOBAL_COOLDOWN_SECONDS


def symbol_cooldown_ok(state) -> bool:
    if not state:
        return True
    last = state.get("last_sent_at") or 0
    return time.time() - last >= SYMBOL_COOLDOWN_SECONDS


def detect_event(analysis: dict, state: dict):
    symbol = analysis["symbol"]
    signal = analysis["signal"]
    price = analysis["price"]

    if not state:
        return {
            "event_type": "init",
            "event_key": f"{symbol}:init:{signal}",
            "should_send": False,
            "event_note": "首次初始化",
        }

    prev_signal = state.get("last_signal")
    prev_support = state.get("last_support")
    prev_resistance = state.get("last_resistance")

    if prev_signal and prev_signal != signal:
        return {
            "event_type": "signal_change",
            "event_key": f"{symbol}:signal:{prev_signal}->{signal}",
            "should_send": True,
            "event_note": f"信号从{prev_signal}切换为{signal}",
        }

    if prev_resistance and price > prev_resistance:
        return {
            "event_type": "breakout",
            "event_key": f"{symbol}:breakout:{round(prev_resistance, 6)}",
            "should_send": True,
            "event_note": "价格突破前一轮短线压力",
        }

    if prev_support and price < prev_support:
        return {
            "event_type": "breakdown",
            "event_key": f"{symbol}:breakdown:{round(prev_support, 6)}",
            "should_send": True,
            "event_note": "价格跌破前一轮短线支撑",
        }

    threshold = FIVE_MIN_MOVE_THRESHOLD.get(symbol, 0.8)
    if analysis["volume_ratio"] >= VOLUME_SPIKE_MULTIPLIER and abs(analysis["last_5m_change_pct"]) >= threshold:
        direction = "up" if analysis["last_5m_change_pct"] > 0 else "down"
        return {
            "event_type": "volume_spike",
            "event_key": f"{symbol}:vol:{direction}:{signal}",
            "should_send": True,
            "event_note": "5分钟成交量明显放大并伴随价格异动",
        }

    return {
        "event_type": "none",
        "event_key": f"{symbol}:none:{signal}",
        "should_send": False,
        "event_note": "无明显触发",
    }


# =========================
# 文案生成
# =========================

SIGNAL_TEXT = {
    "强偏多": "结构明显偏强，买盘承接更主动，短线多头仍占上风。",
    "偏多": "短线结构没有走坏，回踩不破关键位，多头仍有延续空间。",
    "震荡": "短线方向还不清晰，资金更像在等待新的突破信号。",
    "偏空": "反弹力度一般，短线结构偏弱，资金追高意愿不强。",
    "强偏空": "空头节奏更明显，如果不能快速收回关键位，压力还会继续放大。",
}

EVENT_CN = {
    "signal_change": "信号切换",
    "breakout": "突破压力",
    "breakdown": "跌破支撑",
    "volume_spike": "放量异动",
    "none": "常规观察",
    "init": "初始化",
}

TITLE_PREFIX_POOL = {
    "signal_change": ["信号监控", "短线信号", "币圈监控", "行情监控", "主流币监控"],
    "breakout": ["突破监控", "行情异动", "主流币监控", "压力位监控", "短线突破"],
    "breakdown": ["跌破监控", "风险监控", "行情监控", "支撑位监控", "短线回落"],
    "volume_spike": ["放量异动", "异动监控", "成交量监控", "资金异动", "行情监控"],
}


def fallback_comment(analysis: dict, event: dict) -> str:
    signal = analysis["signal"]
    event_note = event.get("event_note", "")
    base = SIGNAL_TEXT.get(signal, "短线结构仍需观察。")

    if event["event_type"] == "breakout":
        return "价格突破前一轮短线压力，结构开始转强，买盘承接比上一轮更主动。"

    if event["event_type"] == "breakdown":
        return "价格跌破前一轮短线支撑后，反弹没有及时跟上，短线结构开始转弱。"

    if event["event_type"] == "volume_spike":
        return f"{event_note}，当前更需要看价格能否在关键区间外站稳。"

    return base


def ai_polish_comment(analysis: dict, event: dict) -> str:
    if not USE_AI_POLISH or not client:
        return fallback_comment(analysis, event)

    prompt = f"""
请根据下面的结构化行情数据，写一句适合Telegram虚拟币行情监控频道发布的短评。

要求：
1. 只写一句中文，35到70字
2. 不要说“保证上涨”“保证下跌”
3. 不要写成投资建议
4. 不要出现“加杠杆”“梭哈”
5. 可以有明确强弱判断
6. 尽量自然带入这些词里的1到2个：币圈监控、行情监控、短线结构、支撑压力、成交量异动、主流币监控
7. 不要输出多余解释
8. 不要使用省略号

数据：
币种：{analysis["display"]}
当前价：{analysis["price"]}
信号：{analysis["signal"]}
事件：{event["event_note"]}
原因：{analysis["reason"]}
5分钟量能倍数：{analysis["volume_ratio"]:.2f}
24小时涨跌幅：{analysis["pct_24h"]:.2f}%
支撑：{analysis["support"]}
压力：{analysis["resistance"]}
""".strip()

    try:
        response = client.responses.create(
            model=MODEL_NAME,
            input=prompt,
        )
        text = (response.output_text or "").strip()
        text = re.sub(r"\s+", " ", text)
        text = text.replace("...", "").replace("……", "")
        if 10 <= len(text) <= 100:
            return text
    except Exception as e:
        print("AI润色失败，使用备用文案:", e)

    return fallback_comment(analysis, event)


def build_invalid_line(analysis: dict) -> str:
    signal = analysis["signal"]
    support = fmt_price(analysis["symbol"], analysis["support"])
    resistance = fmt_price(analysis["symbol"], analysis["resistance"])

    if signal in {"强偏多", "偏多"}:
        return f"失效：跌破{support}"

    if signal in {"强偏空", "偏空"}:
        return f"失效：站回{resistance}"

    return f"突破：站上{resistance}转强，跌破{support}转弱"


def unique_keep_order(items):
    result = []
    for item in items:
        if item and item not in result:
            result.append(item)
    return result


def pick_tags(analysis: dict, event: dict) -> str:
    symbol = analysis["symbol"]
    signal = analysis["signal"]
    event_type = event["event_type"]

    candidates = []
    candidates.extend(SYMBOL_TAGS.get(symbol, []))
    candidates.extend(EVENT_TAGS.get(event_type, []))
    candidates.extend(SIGNAL_TAGS.get(signal, []))
    candidates.extend(GENERAL_TAGS)

    candidates = unique_keep_order(candidates)

    must_have = []
    symbol_tags = SYMBOL_TAGS.get(symbol, [])
    if symbol_tags:
        must_have.append(symbol_tags[0])

    event_tags = EVENT_TAGS.get(event_type, [])
    if event_tags:
        must_have.append(random.choice(event_tags))

    signal_tags = SIGNAL_TAGS.get(signal, [])
    if signal_tags:
        must_have.append(signal_tags[0])

    must_have = unique_keep_order(must_have)

    target_count = random.randint(3, 5)
    chosen = must_have[:]

    pool = [x for x in candidates if x not in chosen]
    random.shuffle(pool)

    for tag in pool:
        if len(chosen) >= target_count:
            break
        chosen.append(tag)

    chosen = chosen[:5]
    return " ".join(chosen)


def build_title_prefix(event: dict) -> str:
    event_type = event["event_type"]
    pool = TITLE_PREFIX_POOL.get(event_type)
    if pool:
        return random.choice(pool)
    return random.choice(["币圈监控", "行情监控", "主流币监控"])


def build_monitor_focus(analysis: dict, event: dict) -> str:
    symbol = analysis["symbol"]
    display = analysis["display"]
    event_type = event["event_type"]

    phrases = [
        "币圈监控",
        "行情监控",
        "短线结构",
        "支撑压力",
        "成交量变化",
        "主流币联动",
    ]

    if symbol == "BTCUSDT":
        phrases.extend(["BTC监控", "比特币行情", "主流币风向"])
    elif symbol == "ETHUSDT":
        phrases.extend(["ETH监控", "以太坊行情", "主流币联动"])
    elif symbol == "SOLUSDT":
        phrases.extend(["SOL监控", "山寨币情绪", "公链生态"])
    elif symbol == "XRPUSDT":
        phrases.extend(["XRP监控", "山寨币行情", "短线波动"])
    elif symbol == "BNBUSDT":
        phrases.extend(["BNB监控", "币安生态", "交易所生态"])

    if event_type == "breakout":
        phrases.extend(["突破监控", "压力位", "短线转强"])
    elif event_type == "breakdown":
        phrases.extend(["跌破监控", "支撑位", "风险监控"])
    elif event_type == "volume_spike":
        phrases.extend(["放量异动", "异动监控", "资金异动"])
    elif event_type == "signal_change":
        phrases.extend(["信号监控", "趋势切换", "多空变化"])

    phrases = unique_keep_order(phrases)
    random.shuffle(phrases)
    selected = phrases[:random.randint(3, 5)]
    return "、".join(selected)


def build_message(analysis: dict, event: dict) -> str:
    symbol = analysis["symbol"]
    display = analysis["display"]
    signal = analysis["signal"]
    event_type = event["event_type"]

    comment = ai_polish_comment(analysis, event)

    price = fmt_price(symbol, analysis["price"])
    support = fmt_price(symbol, analysis["support"])
    resistance = fmt_price(symbol, analysis["resistance"])
    invalid = build_invalid_line(analysis)
    tags = pick_tags(analysis, event)
    prefix = build_title_prefix(event)
    event_cn = EVENT_CN.get(event_type, "行情观察")
    monitor_focus = build_monitor_focus(analysis, event)

    template_id = random.randint(1, 5)

    if template_id == 1:
        return f"""【{prefix}｜{display}】
{comment}

现价：{price}
短线信号：{signal}
支撑：{support}
压力：{resistance}
{invalid}
监控重点：{monitor_focus}

{tags}""".strip()

    if template_id == 2:
        return f"""【{prefix}｜{display}】
{display} 当前进入币圈行情监控视野，短线结构重点看支撑压力和成交量变化。

{comment}

当前价格：{price}
触发事件：{event_cn}
短线结构：{signal}
关键区间：{support} - {resistance}
观察点：{invalid}
监控关键词：{monitor_focus}

{tags}""".strip()

    if template_id == 3:
        return f"""【{prefix}｜{display}】
主流币监控更新：{display} 出现{event_cn}，当前更适合看短线结构是否延续。

价格：{price}
方向：{signal}
支撑/压力：{support} / {resistance}
量能倍数：{analysis["volume_ratio"]:.2f}
风控观察：{invalid}
行情监控重点：{monitor_focus}

{tags}""".strip()

    if template_id == 4:
        return f"""【{prefix}｜{display}】
{comment}

币圈监控结论：
现价 {price}，当前信号为{signal}，短线支撑压力区间在 {support} / {resistance}。

触发原因：{event["event_note"]}
观察重点：{monitor_focus}
{invalid}

{tags}""".strip()

    return f"""【{prefix}｜{display}】
{display} 短线行情异动提醒，当前属于{event_cn}场景，主流币监控重点转向成交量、支撑压力和短线信号变化。

现价：{price}
信号：{signal}
支撑：{support}
压力：{resistance}
失效/转向：{invalid}
关键词：{monitor_focus}

{tags}""".strip()


# =========================
# 撸毛栏目：低频关键词内容 + lm.png
# =========================

LM_TITLE_POOL = [
    "撸毛监控｜空投方向",
    "空投监控｜测试网交互",
    "撸毛情报｜积分任务",
    "空投机会｜早期项目",
    "测试网交互｜任务观察",
    "积分任务｜低成本交互",
]

LM_FOCUS_POOL = [
    ["空投监控", "测试网交互", "积分任务", "Galxe", "Layer3"],
    ["撸毛情报", "钱包交互", "Zealy", "官方Discord", "官方X"],
    ["早期项目", "白名单", "徽章任务", "项目官网", "任务入口"],
    ["低成本交互", "链上交互", "积分系统", "测试网", "空投任务"],
    ["空投机会", "撸毛机会", "项目任务页", "社媒绑定", "交互记录"],
]

LM_BODY_TEMPLATES = [
    """今天的撸毛监控可以重点看测试网交互、积分任务和项目官网任务中心。空投监控不是只看项目热度，更要看任务门槛、钱包交互成本和是否有积分记录。

关注方向：{focus}
适合人群：低成本交互、长期撸毛、早期项目观察""",

    """空投监控方向继续看 Galxe、Layer3、Zealy、项目官网任务页和官方 Discord。撸毛情报重点不是盲目冲，而是筛选有任务体系、有积分记录、有持续更新的项目。

任务重点：{focus}
观察标准：是否需要钱包交互、社媒绑定、测试网操作、积分累计""",

    """撸毛栏目更新：测试网交互、积分任务、白名单任务和徽章任务仍然是常见入口。可以优先关注项目官网任务中心、官方 X 置顶和 Discord 公告区。

关键词方向：{focus}
参与思路：低成本、多记录、少授权、看官方任务页""",

    """空投机会不一定来自热门项目，也可能来自长期积分任务和早期测试网。当前可以把空投监控重点放在任务入口、钱包交互、社媒绑定和积分系统上。

监控重点：{focus}
注意方向：任务是否持续、积分是否记录、规则是否清晰""",
]


def pick_lm_tags() -> str:
    target_count = random.randint(3, 5)
    pool = LM_TAG_POOL[:]
    random.shuffle(pool)
    chosen = unique_keep_order(pool[:target_count])
    return " ".join(chosen[:5])


def get_lm_image_path() -> str:
    path = os.path.join(IMAGES_DIR, LM_IMAGE)
    if os.path.isfile(path):
        return path
    return ""


def build_lm_message() -> str:
    title = random.choice(LM_TITLE_POOL)
    focus = "、".join(random.choice(LM_FOCUS_POOL))
    body = random.choice(LM_BODY_TEMPLATES).format(focus=focus)
    tags = pick_lm_tags()

    return f"""【{title}】
{body}

{tags}""".strip()


def lm_should_send() -> bool:
    if not ENABLE_LM_COLUMN:
        return False

    last = float(get_meta("last_lm_sent_at", "0") or 0)

    if last <= 0:
        return LM_FIRST_RUN_SEND

    return time.time() - last >= LM_POST_INTERVAL_SECONDS


def process_lm_column():
    if not lm_should_send():
        return

    text = build_lm_message()
    photo_path = get_lm_image_path()

    if photo_path:
        resp = send_telegram_photo(photo_path, text)
        if resp.status_code != 200:
            print("撸毛栏目图片发送失败，改为纯文字")
            resp = send_telegram_message(text)
    else:
        resp = send_telegram_message(text)

    if resp.status_code == 200:
        now = time.time()
        set_meta("last_lm_sent_at", str(now))
        record_sent("lm", "", "lm_column")
        print("撸毛栏目已发送")
    else:
        print("撸毛栏目发送失败")


# =========================
# 图片处理
# =========================

def get_symbol_image_path(symbol: str) -> str:
    filename = IMAGE_MAP.get(symbol)
    if not filename:
        return ""

    path = os.path.join(IMAGES_DIR, filename)
    if os.path.isfile(path):
        return path

    return ""


# =========================
# Telegram
# =========================

def safe_caption(text: str) -> str:
    text = (text or "").strip()
    if len(text) <= 1024:
        return text
    return text[:1000].rstrip() + "\n……"


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        data={
            "chat_id": CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=30
    )
    print("sendMessage:", resp.status_code, resp.text)
    return resp


def send_telegram_photo(photo_path: str, caption: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"

    with open(photo_path, "rb") as f:
        resp = requests.post(
            url,
            data={
                "chat_id": CHAT_ID,
                "caption": safe_caption(caption),
            },
            files={
                "photo": f,
            },
            timeout=30
        )

    print("sendPhoto:", resp.status_code, resp.text)
    return resp


def send_signal(symbol: str, text: str):
    photo_path = get_symbol_image_path(symbol)

    if photo_path:
        resp = send_telegram_photo(photo_path, text)
        if resp.status_code == 200:
            return resp

        print("图片发送失败，改为纯文字发送")

    return send_telegram_message(text)


# =========================
# 主流程
# =========================

def process_symbol(symbol: str):
    analysis = analyze_symbol(symbol)
    state = get_state(symbol)
    event = detect_event(analysis, state)

    if not state:
        print(f"{symbol} 初始化：{analysis['signal']}")
        upsert_state(
            symbol=symbol,
            signal=analysis["signal"],
            support=analysis["support"],
            resistance=analysis["resistance"],
        )
        return

    if not event["should_send"]:
        print(f"{symbol} 无触发：{analysis['signal']}")
        upsert_state(
            symbol=symbol,
            signal=analysis["signal"],
            support=analysis["support"],
            resistance=analysis["resistance"],
        )
        return

    if event["event_key"] == state.get("last_event_key"):
        print(f"{symbol} 同事件已发过，跳过：{event['event_key']}")
        upsert_state(
            symbol=symbol,
            signal=analysis["signal"],
            support=analysis["support"],
            resistance=analysis["resistance"],
        )
        return

    if not symbol_cooldown_ok(state):
        print(f"{symbol} 单币冷却中，跳过发送")
        upsert_state(
            symbol=symbol,
            signal=analysis["signal"],
            support=analysis["support"],
            resistance=analysis["resistance"],
        )
        return

    if not global_cooldown_ok():
        print(f"{symbol} 全频道冷却中，跳过发送")
        upsert_state(
            symbol=symbol,
            signal=analysis["signal"],
            support=analysis["support"],
            resistance=analysis["resistance"],
        )
        return

    if not signal_hourly_limit_ok():
        print(f"{symbol} 每小时行情信号达到上限，跳过发送")
        upsert_state(
            symbol=symbol,
            signal=analysis["signal"],
            support=analysis["support"],
            resistance=analysis["resistance"],
            event_key=event["event_key"],
        )
        return

    if not signal_daily_limit_ok():
        print(f"{symbol} 今日行情信号达到上限，跳过发送")
        upsert_state(
            symbol=symbol,
            signal=analysis["signal"],
            support=analysis["support"],
            resistance=analysis["resistance"],
            event_key=event["event_key"],
        )
        return

    message = build_message(analysis, event)
    resp = send_signal(symbol, message)

    now = time.time()

    if resp.status_code == 200:
        print(f"{symbol} 已发送：{analysis['signal']} / {event['event_type']}")
        set_meta("last_global_sent_at", str(now))
        record_sent("signal", symbol, event["event_type"])
        upsert_state(
            symbol=symbol,
            signal=analysis["signal"],
            support=analysis["support"],
            resistance=analysis["resistance"],
            event_key=event["event_key"],
            sent_at=now,
        )
    else:
        print(f"{symbol} 发送失败，但更新状态防止重复刷屏")
        upsert_state(
            symbol=symbol,
            signal=analysis["signal"],
            support=analysis["support"],
            resistance=analysis["resistance"],
            event_key=event["event_key"],
            sent_at=now,
        )


def main():
    if not BOT_TOKEN:
        raise ValueError("缺少环境变量 BOT_TOKEN")

    if not CHAT_ID:
        raise ValueError("缺少环境变量 CHAT_ID")

    init_db()

    print("币圈监控小助手启动成功（关键词增强 + 降频 + 撸毛栏目版）")
    print("监控币种:", ", ".join(SYMBOLS.keys()))
    print("频道:", CHAT_ID)
    print("图片目录:", IMAGES_DIR)
    print("检查间隔:", CHECK_INTERVAL)
    print("单币冷却:", SYMBOL_COOLDOWN_SECONDS)
    print("全频道冷却:", GLOBAL_COOLDOWN_SECONDS)
    print("每日行情信号上限:", MAX_SIGNALS_PER_DAY)
    print("每小时行情信号上限:", MAX_SIGNALS_PER_HOUR)
    print("撸毛栏目:", "开启" if ENABLE_LM_COLUMN else "关闭")
    print("撸毛图片:", LM_IMAGE)

    if not has_any_state():
        print("首次启动：初始化所有币种状态，不发送当前旧信号")

    while True:
        print(f"\n[{datetime.now()}] 开始检查行情")

        for symbol in SYMBOLS.keys():
            try:
                process_symbol(symbol)
                time.sleep(1.2)
            except Exception as e:
                print(f"{symbol} 处理失败:", e)

        try:
            process_lm_column()
        except Exception as e:
            print("撸毛栏目处理失败:", e)

        print(f"休眠 {CHECK_INTERVAL} 秒")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
