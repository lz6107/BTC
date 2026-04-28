import os
import re
import time
import math
import sqlite3
from datetime import datetime

import requests
from openai import OpenAI


# =========================
# 基础配置：主流币实时观察 + 图片版
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

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))
SYMBOL_COOLDOWN_SECONDS = int(os.getenv("SYMBOL_COOLDOWN_SECONDS", "600"))
GLOBAL_COOLDOWN_SECONDS = int(os.getenv("GLOBAL_COOLDOWN_SECONDS", "180"))

USE_AI_POLISH = os.getenv("USE_AI_POLISH", "true").lower() == "true"
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-5.4-nano")

KLINE_LIMIT = int(os.getenv("KLINE_LIMIT", "60"))
IMAGES_DIR = os.getenv("IMAGES_DIR", "images")

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
请根据下面的结构化行情数据，写一句适合Telegram虚拟币观察频道发布的短评。

要求：
1. 只写一句中文，30到55字
2. 不要说“保证上涨”“保证下跌”
3. 不要写成投资建议
4. 不要出现“加杠杆”“梭哈”
5. 可以有明确强弱判断
6. 不要输出多余解释
7. 不要使用省略号

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
        if 10 <= len(text) <= 90:
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


def build_message(analysis: dict, event: dict) -> str:
    symbol = analysis["symbol"]
    display = analysis["display"]
    signal = analysis["signal"]

    comment = ai_polish_comment(analysis, event)

    price = fmt_price(symbol, analysis["price"])
    support = fmt_price(symbol, analysis["support"])
    resistance = fmt_price(symbol, analysis["resistance"])
    invalid = build_invalid_line(analysis)

    return f"""【{display}】
{comment}

现价：{price}
信号：{signal}
支撑：{support}
压力：{resistance}
{invalid}

#{display} #主流币 #{signal}""".strip()


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
                "caption": caption,
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

    message = build_message(analysis, event)
    resp = send_signal(symbol, message)

    now = time.time()

    if resp.status_code == 200:
        print(f"{symbol} 已发送：{analysis['signal']} / {event['event_type']}")
        set_meta("last_global_sent_at", str(now))
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

    print("主流币实时观察机器人启动成功（图片版）")
    print("监控币种:", ", ".join(SYMBOLS.keys()))
    print("频道:", CHAT_ID)
    print("图片目录:", IMAGES_DIR)

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

        print(f"休眠 {CHECK_INTERVAL} 秒")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
