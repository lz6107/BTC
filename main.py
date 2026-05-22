import os
import re
import json
import time
import math
import random
import sqlite3
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
import feedparser
from openai import OpenAI


# =========================
# 币圈监控小助手：重构版
# 10币行情监控 + 新闻背景分析 + 空投/撸毛 RSS + 多关键词曝光
# =========================

BINANCE_BASE_URL = "https://api.binance.com"
COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# 尽量沿用旧参数
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))  # 每5分钟检查一次
SYMBOL_COOLDOWN_SECONDS = int(os.getenv("SYMBOL_COOLDOWN_SECONDS", "14400"))  # 同币默认4小时
GLOBAL_COOLDOWN_SECONDS = int(os.getenv("GLOBAL_COOLDOWN_SECONDS", "900"))  # 全频道15分钟
USE_AI_POLISH = os.getenv("USE_AI_POLISH", "true").lower() == "true"
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-5.4-nano")
KLINE_LIMIT = int(os.getenv("KLINE_LIMIT", "80"))
IMAGES_DIR = os.getenv("IMAGES_DIR", "images")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))

# 新增控频参数
MAX_POSTS_PER_DAY = int(os.getenv("MAX_POSTS_PER_DAY", "30"))
MAX_POSTS_PER_HOUR = int(os.getenv("MAX_POSTS_PER_HOUR", "4"))
MIN_SYMBOL_POSTS_PER_DAY = int(os.getenv("MIN_SYMBOL_POSTS_PER_DAY", "1"))
MAX_SYMBOL_POSTS_PER_DAY = int(os.getenv("MAX_SYMBOL_POSTS_PER_DAY", "3"))
MAX_POSTS_PER_LOOP = int(os.getenv("MAX_POSTS_PER_LOOP", "1"))

# 新闻与空投
ENABLE_NEWS_CONTEXT = os.getenv("ENABLE_NEWS_CONTEXT", "true").lower() == "true"
ENABLE_AIRDROP_RSS = os.getenv("ENABLE_AIRDROP_RSS", "true").lower() == "true"
NEWS_CHECK_INTERVAL = int(os.getenv("NEWS_CHECK_INTERVAL", "1800"))  # 30分钟更新一次RSS
NEWS_MAX_ITEMS_PER_FEED = int(os.getenv("NEWS_MAX_ITEMS_PER_FEED", "6"))
NEWS_LOOKBACK_HOURS = int(os.getenv("NEWS_LOOKBACK_HOURS", "48"))

ENABLE_LM_COLUMN = os.getenv("ENABLE_LM_COLUMN", "true").lower() == "true"
LM_IMAGE = os.getenv("LM_IMAGE", "lm.png")
LM_POST_INTERVAL_SECONDS = int(os.getenv("LM_POST_INTERVAL_SECONDS", "14400"))  # 默认4小时
LM_FIRST_RUN_SEND = os.getenv("LM_FIRST_RUN_SEND", "true").lower() == "true"

# 新增：币圈热词监控栏目
ENABLE_HOTWORD_COLUMN = os.getenv("ENABLE_HOTWORD_COLUMN", "true").lower() == "true"
HOTWORD_IMAGE = os.getenv("HOTWORD_IMAGE", "hotword.png")
HOTWORD_POSTS_PER_DAY = int(os.getenv("HOTWORD_POSTS_PER_DAY", "10"))
HOTWORD_GENERATE_HOUR = int(os.getenv("HOTWORD_GENERATE_HOUR", "8"))
HOTWORD_SEND_TIMES = os.getenv(
    "HOTWORD_SEND_TIMES",
    "09:00,10:30,12:00,13:30,15:00,16:30,18:00,19:30,21:00,22:30"
)
# 热词栏目必须由 RSS 证据驱动：不再用行情榜/内置词硬编。
HOTWORD_EXCLUDE_MONITORED_COINS = os.getenv("HOTWORD_EXCLUDE_MONITORED_COINS", "true").lower() == "true"
HOTWORD_EXTRA_EXCLUDE = os.getenv("HOTWORD_EXTRA_EXCLUDE", "BTC,ETH,BNB,XRP,SOL,TRX,DOGE,ADA,LINK,AVAX")
HOTWORD_MIN_RSS_EVIDENCE = int(os.getenv("HOTWORD_MIN_RSS_EVIDENCE", "1"))
HOTWORD_MAX_EVIDENCE_PER_TERM = int(os.getenv("HOTWORD_MAX_EVIDENCE_PER_TERM", "5"))

# 新增：扩展栏目（不改变原有栏目频率，额外每天 3-5 条）
ENABLE_EXTRA_COLUMNS = os.getenv("ENABLE_EXTRA_COLUMNS", "true").lower() == "true"
EXTRA_COLUMN_POSTS_PER_DAY = int(os.getenv("EXTRA_COLUMN_POSTS_PER_DAY", "4"))
EXTRA_COLUMN_SEND_TIMES = os.getenv("EXTRA_COLUMN_SEND_TIMES", "11:20,14:20,17:20,20:20,22:20")
EXTRA_COLUMN_MIN_INTERVAL_SECONDS = int(os.getenv("EXTRA_COLUMN_MIN_INTERVAL_SECONDS", "900"))
EXTRA_COLUMN_TYPES = os.getenv("EXTRA_COLUMN_TYPES", "exchange,contract,sector,wiki")

EXCHANGE_IMAGE = os.getenv("EXCHANGE_IMAGE", "exchange.png")
CONTRACT_IMAGE = os.getenv("CONTRACT_IMAGE", "contract.png")
SECTOR_IMAGE = os.getenv("SECTOR_IMAGE", "sector.png")
WIKI_IMAGE = os.getenv("WIKI_IMAGE", "wiki.png")

# 动态市值前十
ENABLE_DYNAMIC_TOP_COINS = os.getenv("ENABLE_DYNAMIC_TOP_COINS", "true").lower() == "true"
SYMBOL_REFRESH_INTERVAL = int(os.getenv("SYMBOL_REFRESH_INTERVAL", "21600"))  # 6小时刷新一次
TOP_COIN_COUNT = int(os.getenv("TOP_COIN_COUNT", "10"))

# 首次启动只初始化，不发旧信号
FIRST_RUN_INIT_ONLY = os.getenv("FIRST_RUN_INIT_ONLY", "true").lower() == "true"

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


# =========================
# 备用币种：排除稳定币后的市值大币兜底
# =========================

FALLBACK_SYMBOLS = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "BNBUSDT": "BNB",
    "XRPUSDT": "XRP",
    "SOLUSDT": "SOL",
    "TRXUSDT": "TRX",
    "DOGEUSDT": "DOGE",
    "ADAUSDT": "ADA",
    "LINKUSDT": "LINK",
    "AVAXUSDT": "AVAX",
}

STABLE_SYMBOLS = {
    # 稳定币 / 类美元资产，全部排除，不做行情监控
    "USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "USDS", "BUSD", "PYUSD", "USDD", "FRAX",
    "USDP", "GUSD", "LUSD", "SUSD", "EURC", "EURS", "USD1", "RLUSD",
}

EXCLUDED_TOP_SYMBOLS = {
    # 包装资产 / 质押衍生资产 / 平台积分类资产，不适合做独立行情监控
    "STETH", "WSTETH", "WBTC", "WEETH", "RETH", "CBETH", "EZETH", "METH",
    "LEO", "CRO", "OKB", "KCS",
}

EXCLUDED_NAME_KEYWORDS = [
    "stablecoin", "stable coin", "usd", "tether", "circle", "dai",
    "wrapped bitcoin", "wrapped btc", "staked ether", "liquid staked", "binance usd",
]

# 某些 CoinGecko symbol 与 Binance 交易对可能需要特殊处理，可后续补充
COINGECKO_SYMBOL_ALIAS = {
    "btc": "BTC",
    "eth": "ETH",
    "bnb": "BNB",
    "xrp": "XRP",
    "sol": "SOL",
    "trx": "TRX",
    "doge": "DOGE",
    "ada": "ADA",
    "link": "LINK",
    "avax": "AVAX",
    "ton": "TON",
    "sui": "SUI",
    "dot": "DOT",
    "ltc": "LTC",
    "bch": "BCH",
    "uni": "UNI",
    "near": "NEAR",
    "apt": "APT",
    "pepe": "PEPE",
}

# 动态运行中的币种，会在 main() 初始化
SYMBOLS: Dict[str, str] = FALLBACK_SYMBOLS.copy()


# =========================
# 图片配置
# 你会重新生成图：btc.png / eth.png / bnb.png / xrp.png / sol.png / trx.png / doge.png / ada.png / link.png / avax.png / lm.png / market.png
# =========================

DEFAULT_IMAGE_MAP = {
    "BTCUSDT": "btc.png",
    "ETHUSDT": "eth.png",
    "BNBUSDT": "bnb.png",
    "XRPUSDT": "xrp.png",
    "SOLUSDT": "sol.png",
    "TRXUSDT": "trx.png",
    "DOGEUSDT": "doge.png",
    "ADAUSDT": "ada.png",
    "LINKUSDT": "link.png",
    "AVAXUSDT": "avax.png",
    "SUIUSDT": "sui.png",
    "TONUSDT": "ton.png",
    "DOTUSDT": "dot.png",
    "LTCUSDT": "ltc.png",
    "BCHUSDT": "bch.png",
    "ZECUSDT": "zec.png",
    "UNIUSDT": "uni.png",
    "NEARUSDT": "near.png",
    "APTUSDT": "apt.png",
    "PEPEUSDT": "pepe.png",
}

FALLBACK_MARKET_IMAGE = os.getenv("FALLBACK_MARKET_IMAGE", "market.png")


# =========================
# 阈值配置
# =========================

VOLUME_STRONG_MULTIPLIER = float(os.getenv("VOLUME_STRONG_MULTIPLIER", "1.3"))
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", "1.8"))
STRONG_24H_MOVE = float(os.getenv("STRONG_24H_MOVE", "1.5"))

FIVE_MIN_MOVE_THRESHOLD = {
    "BTCUSDT": 0.45,
    "ETHUSDT": 0.55,
    "BNBUSDT": 0.75,
    "XRPUSDT": 0.90,
    "SOLUSDT": 0.90,
    "TRXUSDT": 0.70,
    "DOGEUSDT": 1.10,
    "ADAUSDT": 0.90,
    "LINKUSDT": 0.90,
    "AVAXUSDT": 1.00,
    "SUIUSDT": 1.10,
    "BCHUSDT": 0.80,
    "ZECUSDT": 1.00,
    "PEPEUSDT": 1.50,
}

PRICE_DECIMALS = {
    "BTCUSDT": 2,
    "ETHUSDT": 2,
    "BNBUSDT": 2,
    "XRPUSDT": 4,
    "SOLUSDT": 2,
    "TRXUSDT": 5,
    "DOGEUSDT": 5,
    "ADAUSDT": 4,
    "LINKUSDT": 3,
    "AVAXUSDT": 3,
    "SUIUSDT": 4,
    "BCHUSDT": 2,
    "ZECUSDT": 2,
    "PEPEUSDT": 8,
}


# =========================
# RSS 新闻源
# =========================

NEWS_RSS_FEEDS = [
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("Cointelegraph", "https://cointelegraph.com/rss"),
    ("Decrypt", "https://decrypt.co/feed"),
    ("CryptoNews-Airdrop", "https://crypto.news/tag/airdrop/feed/"),
    ("DappRadar-Airdrops", "https://dappradar.com/blog/category/airdrops/feed/"),
    ("DappRadar-Rewards", "https://dappradar.com/blog/category/rewards/feed/"),
]

# 扩展栏目新闻源：只作为交易所、合约情绪、山寨板块雷达的素材，不单独刷屏
EXTRA_COLUMN_RSS_FEEDS = [
    ("GoogleNews-BinanceListing", "https://news.google.com/rss/search?q=Binance+listing+Launchpool+crypto&hl=en-US&gl=US&ceid=US:en"),
    ("GoogleNews-OKXListing", "https://news.google.com/rss/search?q=OKX+listing+crypto+exchange&hl=en-US&gl=US&ceid=US:en"),
    ("GoogleNews-CoinbaseListing", "https://news.google.com/rss/search?q=Coinbase+listing+crypto&hl=en-US&gl=US&ceid=US:en"),
    ("GoogleNews-Liquidation", "https://news.google.com/rss/search?q=crypto+liquidation+futures+open+interest&hl=en-US&gl=US&ceid=US:en"),
    ("GoogleNews-MemeAI", "https://news.google.com/rss/search?q=crypto+MEME+AI+tokens+RWA+DePIN&hl=en-US&gl=US&ceid=US:en"),
]

AIRDROP_RSS_FEEDS = [
    ("AirdropAlert", "https://airdropalert.com/feed/rssfeed"),
    ("AirdropsIO", "https://airdrops.io/feed/"),
    ("AirdropsIO-Latest", "https://airdrops.io/latest/feed/"),
    ("AirdropBee", "https://airdropbee.com/feed/"),
    ("AirdropBee-Latest", "https://airdropbee.com/latest-airdrops/feed/"),
]


# =========================
# 关键词池：每条标签 3-5 个，最多 5 个
# =========================

GENERAL_TAGS = [
    "#币圈监控", "#行情监控", "#加密货币", "#主流币", "#短线信号",
    "#行情异动", "#支撑压力", "#成交量", "#短线结构", "#市场监控",
    "#币圈行情", "#价格监控", "#趋势观察", "#资金情绪", "#多空变化",
    "#主流币监控", "#短线行情", "#市场情绪", "#盘面观察", "#行情分析",
]

EVENT_TAGS = {
    "signal_change": ["#信号监控", "#趋势切换", "#短线观察", "#多空变化", "#行情监控", "#短线信号"],
    "breakout": ["#突破监控", "#放量突破", "#压力位", "#短线转强", "#主流币异动", "#行情异动"],
    "breakdown": ["#跌破监控", "#支撑位", "#短线转弱", "#风险监控", "#行情回落", "#下跌监控"],
    "volume_spike": ["#放量异动", "#成交量放大", "#异动监控", "#资金异动", "#短线波动", "#行情异动"],
    "daily_coverage": ["#每日监控", "#主流币监控", "#行情观察", "#短线结构", "#币圈监控"],
    "news_driven": ["#新闻驱动", "#市场消息", "#行情分析", "#加密新闻", "#币圈监控"],
    "hotword": ["#币圈热词", "#热词监控", "#热搜币", "#行情热词", "#币圈监控"],
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

NEWS_CATEGORY_TAGS = {
    "btc": ["#BTC", "#比特币", "#BTC监控", "#比特币行情"],
    "eth": ["#ETH", "#以太坊", "#ETH监控", "#以太坊行情"],
    "altcoin": ["#山寨币", "#山寨雷达", "#MEME", "#AI币", "#公链生态"],
    "macro": ["#宏观", "#ETF", "#SEC", "#美联储", "#监管"],
    "exchange": ["#交易所监控", "#Binance", "#OKX", "#Coinbase", "#新币上线"],
    "onchain": ["#链上监控", "#巨鲸监控", "#资金流向", "#钱包监控", "#交易所流入"],
    "airdrop": ["#空投", "#空投监控", "#撸毛情报", "#测试网交互", "#积分任务"],
}

# 正文关键词池：不用全变成标签，放在“关键词观察”里，增加搜索命中但不堆标签
KEYWORD_TEXT_POOL = [
    "币圈监控", "行情监控", "主流币监控", "短线行情", "行情异动",
    "支撑压力", "成交量变化", "短线结构", "多空变化", "市场情绪",
    "BTC监控", "ETH监控", "山寨币监控", "交易所监控", "链上监控",
    "新闻驱动", "资金情绪", "趋势观察", "盘面观察", "风险监控",
]

HOTWORD_TAG_POOL = [
    "#币圈热词", "#热词监控", "#热搜币", "#行情热词", "#币圈监控",
    "#行情监控", "#主流币", "#山寨币", "#加密货币", "#市场情绪",
    "#BTC", "#ETH", "#SOL", "#BNB", "#XRP", "#DOGE", "#ADA", "#LINK", "#AVAX",
    "#比特币行情", "#以太坊行情", "#主流币监控", "#山寨币监控", "#MEME",
    "#ETF", "#SEC", "#Binance", "#OKX", "#交易所监控",
    "#链上监控", "#巨鲸监控", "#资金流向", "#合约", "#爆仓监控",
]

HOTWORD_TEXT_POOL = [
    "BTC", "比特币行情", "ETH", "以太坊行情", "SOL", "BNB", "XRP", "DOGE",
    "山寨币", "MEME", "AI币", "主流币监控", "行情监控", "币圈监控",
    "交易所监控", "Binance", "OKX", "ETF", "SEC", "美联储", "监管",
    "链上监控", "巨鲸监控", "资金流向", "合约情绪", "爆仓监控",
    "支撑压力", "成交量", "短线结构", "市场情绪", "行情异动",
]

HOTWORD_BLOCK_KEYWORDS = [
    "airdrop", "airdrops", "claim", "eligible", "eligibility", "snapshot",
    "testnet", "galxe", "layer3", "zealy", "faucet", "whitelist", "waitlist",
    "空投", "撸毛", "测试网", "积分任务", "交互任务", "白名单", "任务入口",
]


# =========================
# 数据库
# =========================

def init_db():
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS coin_state (
            symbol TEXT PRIMARY KEY,
            display TEXT,
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
            title TEXT,
            created_at REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS news_items (
            link TEXT PRIMARY KEY,
            fingerprint TEXT,
            source TEXT,
            title TEXT,
            summary TEXT,
            categories TEXT,
            symbols TEXT,
            published_at REAL,
            used_count INTEGER DEFAULT 0,
            created_at REAL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS hotword_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_key TEXT NOT NULL,
            slot_no INTEGER NOT NULL,
            send_time TEXT NOT NULL,
            title TEXT,
            hotword TEXT,
            content TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at REAL,
            sent_at REAL,
            UNIQUE(date_key, slot_no)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS extra_column_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_key TEXT NOT NULL,
            slot_no INTEGER NOT NULL,
            send_time TEXT NOT NULL,
            column_type TEXT NOT NULL,
            title TEXT,
            content TEXT NOT NULL,
            image_name TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at REAL,
            sent_at REAL,
            UNIQUE(date_key, slot_no)
        )
    """)

    conn.commit()
    conn.close()


def get_state(symbol: str):
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, display, last_signal, last_support, last_resistance, last_event_key, last_sent_at, updated_at
        FROM coin_state
        WHERE symbol = ?
    """, (symbol,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "symbol": row[0],
        "display": row[1],
        "last_signal": row[2],
        "last_support": row[3],
        "last_resistance": row[4],
        "last_event_key": row[5],
        "last_sent_at": row[6] or 0,
        "updated_at": row[7] or 0,
    }


def upsert_state(symbol: str, display: str, signal: str, support: float, resistance: float,
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
        INSERT INTO coin_state (
            symbol, display, last_signal, last_support, last_resistance,
            last_event_key, last_sent_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            display = excluded.display,
            last_signal = excluded.last_signal,
            last_support = excluded.last_support,
            last_resistance = excluded.last_resistance,
            last_event_key = excluded.last_event_key,
            last_sent_at = excluded.last_sent_at,
            updated_at = excluded.updated_at
    """, (
        symbol, display, signal, support, resistance,
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
    cur.execute("SELECT COUNT(*) FROM coin_state")
    count = cur.fetchone()[0]
    conn.close()
    return count > 0


def record_sent(kind: str, symbol: str = "", event_type: str = "", title: str = ""):
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sent_log(kind, symbol, event_type, title, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (kind, symbol, event_type, title, time.time()))
    conn.commit()
    conn.close()


def count_sent_since(kind: Optional[str], since_ts: float, symbol: Optional[str] = None) -> int:
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()

    query = "SELECT COUNT(*) FROM sent_log WHERE created_at >= ?"
    params = [since_ts]

    if kind:
        query += " AND kind = ?"
        params.append(kind)

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)

    cur.execute(query, tuple(params))
    count = cur.fetchone()[0]
    conn.close()
    return count


def today_start_ts() -> float:
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


def symbol_posts_today(symbol: str) -> int:
    return count_sent_since("market", today_start_ts(), symbol=symbol)


def total_posts_today() -> int:
    # 原有栏目频率保持不变：新增扩展栏目不占用原全局每日额度
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM sent_log
        WHERE created_at >= ?
          AND kind != 'extra_column'
    """, (today_start_ts(),))
    count = cur.fetchone()[0]
    conn.close()
    return count


def total_posts_last_hour() -> int:
    # 原有栏目频率保持不变：新增扩展栏目不占用原每小时额度
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM sent_log
        WHERE created_at >= ?
          AND kind != 'extra_column'
    """, (time.time() - 3600,))
    count = cur.fetchone()[0]
    conn.close()
    return count


def extra_column_posts_today() -> int:
    return count_sent_since("extra_column", today_start_ts())


def current_date_key() -> str:
    return datetime.now().strftime("%Y%m%d")


def parse_hhmm_to_minutes(hhmm: str) -> int:
    try:
        h, m = [int(x) for x in hhmm.strip().split(":", 1)]
        return h * 60 + m
    except Exception:
        return 0


def hotword_send_times() -> List[str]:
    raw = [x.strip() for x in HOTWORD_SEND_TIMES.split(",") if x.strip()]
    if not raw:
        raw = ["09:00", "10:30", "12:00", "13:30", "15:00", "16:30", "18:00", "19:30", "21:00", "22:30"]

    # 去重、按时间排序，最后截到 HOTWORD_POSTS_PER_DAY
    seen = []
    for x in raw:
        if re.match(r"^\d{1,2}:\d{2}$", x) and x not in seen:
            seen.append(x)

    seen.sort(key=parse_hhmm_to_minutes)
    return seen[:HOTWORD_POSTS_PER_DAY]


def extra_column_send_times() -> List[str]:
    raw = [x.strip() for x in EXTRA_COLUMN_SEND_TIMES.split(",") if x.strip()]
    if not raw:
        raw = ["11:20", "14:20", "17:20", "20:20", "22:20"]

    seen = []
    for x in raw:
        if re.match(r"^\d{1,2}:\d{2}$", x) and x not in seen:
            seen.append(x)

    seen.sort(key=parse_hhmm_to_minutes)
    return seen[:max(1, EXTRA_COLUMN_POSTS_PER_DAY)]


def enabled_extra_column_types() -> List[str]:
    allowed = {"exchange", "contract", "sector", "wiki"}
    result = []
    for x in split_env_list(EXTRA_COLUMN_TYPES):
        x = x.lower().strip()
        if x in allowed and x not in result:
            result.append(x)
    return result or ["exchange", "contract", "sector", "wiki"]


def insert_news_item(item: dict):
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO news_items(
            link, fingerprint, source, title, summary, categories, symbols,
            published_at, used_count, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
    """, (
        item["link"],
        item["fingerprint"],
        item["source"],
        item["title"],
        item["summary"],
        json.dumps(item["categories"], ensure_ascii=False),
        json.dumps(item["symbols"], ensure_ascii=False),
        item.get("published_at") or time.time(),
        time.time(),
    ))
    conn.commit()
    conn.close()


def mark_news_used(link: str):
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("UPDATE news_items SET used_count = used_count + 1 WHERE link = ?", (link,))
    conn.commit()
    conn.close()


def fetch_news_context(symbol: str, display: str, limit: int = 2) -> List[dict]:
    cutoff = time.time() - NEWS_LOOKBACK_HOURS * 3600
    display_lower = display.lower()

    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT link, source, title, summary, categories, symbols, used_count, published_at
        FROM news_items
        WHERE published_at >= ?
        ORDER BY used_count ASC, published_at DESC
        LIMIT 80
    """, (cutoff,))
    rows = cur.fetchall()
    conn.close()

    scored = []
    for row in rows:
        link, source, title, summary, categories_raw, symbols_raw, used_count, published_at = row
        try:
            categories = json.loads(categories_raw or "[]")
        except Exception:
            categories = []
        try:
            symbols = json.loads(symbols_raw or "[]")
        except Exception:
            symbols = []

        text = f"{title} {summary}".lower()
        score = 0

        if display in symbols or display_lower in text:
            score += 5

        if display in {"BTC", "ETH"} and display_lower in text:
            score += 4

        if display in {"SOL", "DOGE", "ADA", "LINK", "AVAX", "SUI", "PEPE", "XRP", "BNB", "TRX"} and "altcoin" in categories:
            score += 2

        if "macro" in categories:
            score += 1
        if "exchange" in categories:
            score += 1
        if "onchain" in categories:
            score += 1

        if score > 0:
            scored.append({
                "link": link,
                "source": source,
                "title": title,
                "summary": summary,
                "categories": categories,
                "symbols": symbols,
                "used_count": used_count,
                "published_at": published_at,
                "score": score,
            })

    scored.sort(key=lambda x: (x["score"], -x["used_count"], x["published_at"]), reverse=True)
    return scored[:limit]


def fetch_airdrop_news(limit: int = 3) -> List[dict]:
    cutoff = time.time() - NEWS_LOOKBACK_HOURS * 3600
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT link, source, title, summary, categories, symbols, used_count, published_at
        FROM news_items
        WHERE published_at >= ?
          AND categories LIKE '%airdrop%'
        ORDER BY used_count ASC, published_at DESC
        LIMIT ?
    """, (cutoff, limit))
    rows = cur.fetchall()
    conn.close()

    result = []
    for row in rows:
        link, source, title, summary, categories_raw, symbols_raw, used_count, published_at = row
        result.append({
            "link": link,
            "source": source,
            "title": title,
            "summary": summary,
            "used_count": used_count,
            "published_at": published_at,
        })
    return result


# =========================
# 工具函数
# =========================

def clean_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.I)
    text = re.sub(r"<.*?>", "", text, flags=re.S)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def short_text(text: str, max_len: int = 420) -> str:
    text = clean_html(text)
    if len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + ""


def make_fingerprint(text: str) -> str:
    normalized = (text or "").lower()
    normalized = re.sub(r"[^a-z0-9\u4e00-\u9fa5]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return hashlib.md5(normalized.encode("utf-8")).hexdigest() if normalized else ""


def request_json(url: str, params: Optional[dict] = None, timeout: int = REQUEST_TIMEOUT):
    resp = requests.get(url, params=params or {}, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    return resp.json()


def avg(values):
    values = [v for v in values if v is not None and not math.isnan(v)]
    if not values:
        return 0
    return sum(values) / len(values)


def fmt_price(symbol: str, value: float) -> str:
    decimals = PRICE_DECIMALS.get(symbol, 4)
    return f"{value:.{decimals}f}"


def unique_keep_order(items: List[str]) -> List[str]:
    result = []
    for item in items:
        if item and item not in result:
            result.append(item)
    return result


def parse_entry_time(entry) -> float:
    for attr in ["published_parsed", "updated_parsed"]:
        value = getattr(entry, attr, None)
        if value:
            try:
                return time.mktime(value)
            except Exception:
                pass
    return time.time()


# =========================
# 动态获取市值前十可监控币种
# =========================

def binance_symbol_exists(symbol: str) -> bool:
    try:
        data = request_json(f"{BINANCE_BASE_URL}/api/v3/ticker/price", {"symbol": symbol}, timeout=8)
        return bool(data and data.get("price"))
    except Exception:
        return False


def fetch_top_market_symbols() -> Dict[str, str]:
    if not ENABLE_DYNAMIC_TOP_COINS:
        return FALLBACK_SYMBOLS.copy()

    try:
        data = request_json(
            COINGECKO_MARKETS_URL,
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 30,
                "page": 1,
                "sparkline": "false",
            },
            timeout=15,
        )

        result = {}
        for item in data:
            raw_symbol = str(item.get("symbol", "")).lower().strip()
            coin_name = str(item.get("name", "")).lower().strip()
            coin_id = str(item.get("id", "")).lower().strip()
            if not raw_symbol:
                continue

            display = COINGECKO_SYMBOL_ALIAS.get(raw_symbol, raw_symbol.upper()).upper()

            # 去除稳定币、包装币、质押衍生币，避免频道内容被稳定币稀释
            if display in STABLE_SYMBOLS or display in EXCLUDED_TOP_SYMBOLS:
                continue
            if any(k in coin_name or k in coin_id for k in EXCLUDED_NAME_KEYWORDS):
                continue

            pair = f"{display}USDT"
            if pair in result:
                continue

            if binance_symbol_exists(pair):
                result[pair] = display.upper()

            if len(result) >= TOP_COIN_COUNT:
                break

        if len(result) >= 5:
            print("动态市值币种:", result)
            return result

        print("动态市值币种不足，使用备用列表")
        return FALLBACK_SYMBOLS.copy()

    except Exception as e:
        print("获取市值前十失败，使用备用列表:", e)
        return FALLBACK_SYMBOLS.copy()


def refresh_symbols_if_needed(force: bool = False):
    global SYMBOLS

    last = float(get_meta("last_symbol_refresh_at", "0") or 0)
    if not force and time.time() - last < SYMBOL_REFRESH_INTERVAL:
        return

    new_symbols = fetch_top_market_symbols()
    if new_symbols:
        SYMBOLS = new_symbols
        set_meta("last_symbol_refresh_at", str(time.time()))
        set_meta("current_symbols", json.dumps(SYMBOLS, ensure_ascii=False))
        print("当前监控币种:", ", ".join([f"{v}({k})" for k, v in SYMBOLS.items()]))


# =========================
# Binance 行情分析
# =========================

def fetch_klines(symbol: str, interval: str, limit: int = 80):
    data = request_json(
        f"{BINANCE_BASE_URL}/api/v3/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )
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
    data = request_json(f"{BINANCE_BASE_URL}/api/v3/ticker/24hr", {"symbol": symbol})
    return {
        "last_price": float(data["lastPrice"]),
        "price_change_percent": float(data["priceChangePercent"]),
        "volume": float(data["volume"]),
        "quote_volume": float(data["quoteVolume"]),
    }


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


def analyze_symbol(symbol: str, display: str):
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

    # 1小时涨跌：用最近12根完成的5mK线估算
    recent_12 = candles_5[-12:] if len(candles_5) >= 12 else candles_5
    change_1h = 0
    if recent_12 and recent_12[0]["open"] > 0:
        change_1h = (recent_12[-1]["close"] - recent_12[0]["open"]) / recent_12[0]["open"] * 100

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
        "display": display,
        "price": current_price,
        "pct_24h": pct_24h,
        "change_1h": change_1h,
        "ma5_20": ma5_20,
        "ma15_20": ma15_20,
        "support": support,
        "resistance": resistance,
        "volume_ratio": volume_ratio,
        "last_5m_change_pct": last_5m_change_pct,
        "signal": signal,
        "score": score,
        "reason": "，".join(reasons[:5]),
    }


# =========================
# 事件判断与控频
# =========================

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
            "priority": 0,
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
            "priority": 70,
        }

    if prev_resistance and price > prev_resistance:
        return {
            "event_type": "breakout",
            "event_key": f"{symbol}:breakout:{round(prev_resistance, 6)}",
            "should_send": True,
            "event_note": "价格突破前一轮短线压力",
            "priority": 85,
        }

    if prev_support and price < prev_support:
        return {
            "event_type": "breakdown",
            "event_key": f"{symbol}:breakdown:{round(prev_support, 6)}",
            "should_send": True,
            "event_note": "价格跌破前一轮短线支撑",
            "priority": 85,
        }

    threshold = FIVE_MIN_MOVE_THRESHOLD.get(symbol, 0.9)
    if analysis["volume_ratio"] >= VOLUME_SPIKE_MULTIPLIER and abs(analysis["last_5m_change_pct"]) >= threshold:
        direction = "up" if analysis["last_5m_change_pct"] > 0 else "down"
        return {
            "event_type": "volume_spike",
            "event_key": f"{symbol}:vol:{direction}:{signal}",
            "should_send": True,
            "event_note": "5分钟成交量明显放大并伴随价格异动",
            "priority": 80,
        }

    return {
        "event_type": "none",
        "event_key": f"{symbol}:none:{signal}",
        "should_send": False,
        "event_note": "无明显触发",
        "priority": 0,
    }


def global_cooldown_ok() -> bool:
    last = float(get_meta("last_global_sent_at", "0") or 0)
    return time.time() - last >= GLOBAL_COOLDOWN_SECONDS


def symbol_cooldown_ok(symbol: str) -> bool:
    state = get_state(symbol)
    if not state:
        return True
    last = state.get("last_sent_at") or 0
    return time.time() - last >= SYMBOL_COOLDOWN_SECONDS


def global_limits_ok() -> bool:
    if total_posts_today() >= MAX_POSTS_PER_DAY:
        print("今日全频道发送达到上限")
        return False
    if total_posts_last_hour() >= MAX_POSTS_PER_HOUR:
        print("最近1小时全频道发送达到上限")
        return False
    if not global_cooldown_ok():
        print("全频道冷却中")
        return False
    return True


def symbol_limits_ok(symbol: str) -> bool:
    if symbol_posts_today(symbol) >= MAX_SYMBOL_POSTS_PER_DAY:
        print(f"{symbol} 今日已达单币上限")
        return False
    if not symbol_cooldown_ok(symbol):
        print(f"{symbol} 单币冷却中")
        return False
    return True


# =========================
# 新闻抓取与分类
# =========================

NEWS_SKIP_KEYWORDS = [
    "price prediction", "sponsored", "advertisement", "casino", "gambling", "betting",
    "press release", "opinion", "podcast", "newsletter", "watch live", "live updates",
]

STABLECOIN_NEWS_KEYWORDS = [
    "stablecoin", "stablecoins", "usdt", "usdc", "tether", "circle", "dai",
    "fdusd", "usde", "usds", "pyusd", "busd", "frxusd", "frax",
]

AIRDROP_KEYWORDS = [
    "airdrop", "airdrops", "claim", "eligible", "eligibility", "snapshot", "points",
    "quest", "quests", "testnet", "galxe", "layer3", "zealy", "faucet", "reward",
    "rewards", "retroactive", "incentive", "campaign", "whitelist", "waitlist",
    "badge", "nft", "任务", "空投", "撸毛", "积分", "交互",
]


def classify_news(title: str, summary: str) -> Tuple[List[str], List[str]]:
    text = f"{title} {summary}".lower()
    categories = []
    symbols = []

    for symbol, display in SYMBOLS.items():
        d = display.lower()
        if d in text or display in title:
            symbols.append(display)

    if any(k in text for k in ["bitcoin", "btc", "比特币"]):
        categories.append("btc")
        if "BTC" not in symbols:
            symbols.append("BTC")

    if any(k in text for k in ["ethereum", "eth", "ether", "以太坊"]):
        categories.append("eth")
        if "ETH" not in symbols:
            symbols.append("ETH")

    if any(k in text for k in ["solana", "sol", "xrp", "bnb", "doge", "ada", "link", "avax", "sui", "memecoin", "meme", "altcoin", "altcoins"]):
        categories.append("altcoin")

    if any(k in text for k in ["sec", "etf", "fed", "federal reserve", "interest rate", "rate cut", "inflation", "cpi", "regulation", "lawsuit"]):
        categories.append("macro")

    if any(k in text for k in ["binance", "okx", "coinbase", "kraken", "listing", "launchpool", "launchpad", "exchange"]):
        categories.append("exchange")

    if any(k in text for k in ["whale", "wallet", "on-chain", "onchain", "inflow", "outflow", "staking", "unlock", "token unlock", "liquidation"]):
        categories.append("onchain")

    if any(k in text for k in AIRDROP_KEYWORDS):
        categories.append("airdrop")

    if not categories:
        categories.append("general")

    return unique_keep_order(categories), unique_keep_order(symbols)


def should_skip_news(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()
    if any(k in text for k in NEWS_SKIP_KEYWORDS):
        return True

    # 用户要求去除稳定币：稳定币新闻不作为行情背景，避免搜索词和频道画像跑偏
    if any(k in text for k in STABLECOIN_NEWS_KEYWORDS):
        return True

    return False


def update_news_cache(force: bool = False):
    if not ENABLE_NEWS_CONTEXT and not ENABLE_AIRDROP_RSS:
        return

    last = float(get_meta("last_news_check_at", "0") or 0)
    if not force and time.time() - last < NEWS_CHECK_INTERVAL:
        return

    feeds = NEWS_RSS_FEEDS[:]
    if ENABLE_EXTRA_COLUMNS:
        feeds.extend(EXTRA_COLUMN_RSS_FEEDS)
    if ENABLE_AIRDROP_RSS:
        feeds.extend(AIRDROP_RSS_FEEDS)

    print("开始更新新闻 RSS，共", len(feeds), "个源")

    added = 0
    for source, url in feeds:
        try:
            feed = feedparser.parse(url)
            entries = list(feed.entries[:NEWS_MAX_ITEMS_PER_FEED])
            for entry in entries:
                title = clean_html(getattr(entry, "title", ""))
                link = getattr(entry, "link", "").strip()
                summary = clean_html(getattr(entry, "summary", "") or getattr(entry, "description", ""))
                summary = short_text(summary, 500)

                if not title or not link:
                    continue

                if should_skip_news(title, summary):
                    continue

                categories, symbols = classify_news(title, summary)
                fingerprint = make_fingerprint(title)

                item = {
                    "source": source,
                    "link": link,
                    "fingerprint": fingerprint,
                    "title": title,
                    "summary": summary,
                    "categories": categories,
                    "symbols": symbols,
                    "published_at": parse_entry_time(entry),
                }
                insert_news_item(item)
                added += 1
        except Exception as e:
            print(f"RSS 解析失败 {source} {url}: {e}")

    set_meta("last_news_check_at", str(time.time()))
    print("新闻 RSS 更新完成，尝试入库条数:", added)


# =========================
# 标签与内容生成
# =========================

EVENT_CN = {
    "signal_change": "信号切换",
    "breakout": "突破压力",
    "breakdown": "跌破支撑",
    "volume_spike": "放量异动",
    "daily_coverage": "每日监控",
    "news_driven": "新闻驱动",
    "none": "常规观察",
    "init": "初始化",
}

TITLE_PREFIX_POOL = {
    "signal_change": ["信号监控", "短线信号", "币圈监控", "行情监控", "主流币监控"],
    "breakout": ["突破监控", "行情异动", "主流币监控", "压力位监控", "短线突破"],
    "breakdown": ["跌破监控", "风险监控", "行情监控", "支撑位监控", "短线回落"],
    "volume_spike": ["放量异动", "异动监控", "成交量监控", "资金异动", "行情监控"],
    "daily_coverage": ["每日监控", "币圈监控", "行情监控", "主流币监控", "短线观察"],
    "news_driven": ["新闻驱动", "行情分析", "市场消息", "币圈监控", "行情监控"],
}

SIGNAL_TEXT = {
    "强偏多": "结构明显偏强，买盘承接更主动，短线多头仍占上风。",
    "偏多": "短线结构没有走坏，回踩不破关键位，多头仍有延续空间。",
    "震荡": "短线方向还不清晰，资金更像在等待新的突破信号。",
    "偏空": "反弹力度一般，短线结构偏弱，资金追高意愿不强。",
    "强偏空": "空头节奏更明显，如果不能快速收回关键位，压力还会继续放大。",
}


def symbol_tags(symbol: str, display: str) -> List[str]:
    return [f"#{display}", f"#{display}监控", f"#{display}行情"]


def pick_tags(analysis: dict, event: dict, news_context: List[dict]) -> str:
    symbol = analysis["symbol"]
    display = analysis["display"]
    signal = analysis["signal"]
    event_type = event["event_type"]

    candidates = []
    candidates.extend(symbol_tags(symbol, display))
    candidates.extend(EVENT_TAGS.get(event_type, []))
    candidates.extend(SIGNAL_TAGS.get(signal, []))
    candidates.extend(GENERAL_TAGS)

    for news in news_context:
        for category in news.get("categories", []):
            candidates.extend(NEWS_CATEGORY_TAGS.get(category, []))

    candidates = unique_keep_order(candidates)

    must_have = [f"#{display}"]
    if EVENT_TAGS.get(event_type):
        must_have.append(random.choice(EVENT_TAGS[event_type]))

    if signal in SIGNAL_TAGS:
        must_have.append(SIGNAL_TAGS[signal][0])

    must_have = unique_keep_order(must_have)

    target_count = random.randint(3, 5)
    chosen = must_have[:]
    pool = [x for x in candidates if x not in chosen]
    random.shuffle(pool)

    for tag in pool:
        if len(chosen) >= target_count:
            break
        chosen.append(tag)

    return " ".join(chosen[:5])


def build_title_prefix(event: dict) -> str:
    return random.choice(TITLE_PREFIX_POOL.get(event["event_type"], ["币圈监控", "行情监控", "主流币监控"]))


def build_keyword_text(analysis: dict, event: dict, news_context: List[dict]) -> str:
    """正文里的关键词观察：不超过 6 个，提升搜索命中，但排版保持干净。"""
    display = analysis.get("display", "")
    event_type = event.get("event_type", "")

    pool = KEYWORD_TEXT_POOL[:]
    if display:
        pool.extend([f"{display}监控", f"{display}行情"])

    if event_type == "breakout":
        pool.extend(["突破监控", "压力位", "短线转强"])
    elif event_type == "breakdown":
        pool.extend(["跌破监控", "支撑位", "短线转弱"])
    elif event_type == "volume_spike":
        pool.extend(["放量异动", "成交量放大", "资金异动"])
    elif event_type == "signal_change":
        pool.extend(["信号监控", "趋势切换", "短线信号"])

    for n in news_context:
        for category in n.get("categories", []):
            if category == "macro":
                pool.extend(["宏观快讯", "ETF", "监管"])
            elif category == "exchange":
                pool.extend(["交易所监控", "新币上线", "Binance"])
            elif category == "onchain":
                pool.extend(["链上监控", "巨鲸监控", "资金流向"])
            elif category == "airdrop":
                pool.extend(["空投监控", "撸毛情报", "积分任务"])

    pool = unique_keep_order(pool)
    random.shuffle(pool)
    return "、".join(pool[:random.randint(4, 6)])


def build_invalid_line(analysis: dict) -> str:
    signal = analysis["signal"]
    support = fmt_price(analysis["symbol"], analysis["support"])
    resistance = fmt_price(analysis["symbol"], analysis["resistance"])

    if signal in {"强偏多", "偏多"}:
        return f"失效观察：跌破 {support}"

    if signal in {"强偏空", "偏空"}:
        return f"转强观察：站回 {resistance}"

    return f"区间观察：站上 {resistance} 转强，跌破 {support} 转弱"


def format_news_background(news_context: List[dict]) -> str:
    if not news_context:
        return "暂无强相关新闻，当前主要参考价格结构、成交量和支撑压力变化。"

    parts = []
    for n in news_context[:2]:
        title = n.get("title", "")
        source = n.get("source", "")
        if title:
            parts.append(f"{source} 提到：{title}")

    return "；".join(parts) if parts else "暂无强相关新闻，当前主要参考价格结构和成交量变化。"


def ai_generate_market_content(analysis: dict, event: dict, news_context: List[dict], tags: str) -> Optional[str]:
    if not USE_AI_POLISH or not client:
        return None

    symbol = analysis["symbol"]
    display = analysis["display"]
    prefix = build_title_prefix(event)
    price = fmt_price(symbol, analysis["price"])
    support = fmt_price(symbol, analysis["support"])
    resistance = fmt_price(symbol, analysis["resistance"])
    event_cn = EVENT_CN.get(event["event_type"], "行情观察")
    news_bg = format_news_background(news_context)
    keyword_text = build_keyword_text(analysis, event, news_context)

    prompt = f"""
请为 Telegram 币圈频道生成一条内容，排版必须清爽，不要写得像长文章。

固定格式必须如下：
【{prefix}｜{display}】

行情：1-2句，说明当前短线结构和事件。
新闻：1句，结合新闻背景，没有强相关新闻就写主要看价格结构和成交量。
数据：现价 {price}｜1h {analysis['change_1h']:.2f}%｜24h {analysis['pct_24h']:.2f}%｜支撑 {support}｜压力 {resistance}｜信号 {analysis['signal']}
观察：1句，说明支撑压力、成交量或失效/转强条件。
关键词：{keyword_text}

{tags}

硬性要求：
1. 不要使用 Markdown 加粗，不要项目符号，不要编号
2. 总字数控制在 260-420 个中文字符
3. 不要投资建议，不要喊单，不要承诺涨跌
4. 不要出现稳定币相关内容，例如 USDT、USDC、稳定币
5. 所有判断必须来自“行情数据”或“新闻背景”，没有新闻背景就明确写“主要看价格结构和成交量”，不要编造消息面
6. 正文自然出现 4-6 个搜索关键词，例如币圈监控、行情监控、主流币监控、短线结构、支撑压力、成交量、新闻驱动、行情异动
7. 最后一行必须原样使用这些标签：{tags}

数据参考：
币种：{display}
事件：{event_cn}
事件说明：{event.get('event_note')}
判断原因：{analysis['reason']}
新闻背景：{news_bg}
观察点：{build_invalid_line(analysis)}
""".strip()

    try:
        response = client.responses.create(model=MODEL_NAME, input=prompt)
        text = (response.output_text or "").strip()
        text = text.replace("...", "").replace("……", "")
        # 清理可能出现的多余空行，让排版更稳
        text = re.sub(r"\n{3,}", "\n\n", text)
        if 50 <= len(text) <= 1200:
            return text
    except Exception as e:
        print("AI 生成行情文案失败，使用备用模板:", e)

    return None


def fallback_market_content(analysis: dict, event: dict, news_context: List[dict], tags: str) -> str:
    symbol = analysis["symbol"]
    display = analysis["display"]
    prefix = build_title_prefix(event)
    price = fmt_price(symbol, analysis["price"])
    support = fmt_price(symbol, analysis["support"])
    resistance = fmt_price(symbol, analysis["resistance"])
    event_cn = EVENT_CN.get(event["event_type"], "行情观察")
    news_bg = format_news_background(news_context)
    base = SIGNAL_TEXT.get(analysis["signal"], "短线结构仍需观察。")
    keyword_text = build_keyword_text(analysis, event, news_context)

    # 统一成清爽排版：不再多段乱跳，但标题、关键词、标签会变化，避免一成不变
    return f"""【{prefix}｜{display}】

行情：{display} 当前触发{event_cn}，短线信号为 {analysis['signal']}。{base}
新闻：{news_bg}
数据：现价 {price}｜1h {analysis['change_1h']:.2f}%｜24h {analysis['pct_24h']:.2f}%｜支撑 {support}｜压力 {resistance}
观察：{build_invalid_line(analysis)}，重点看成交量变化和支撑压力是否继续有效。
关键词：{keyword_text}

{tags}""".strip()


def build_market_message(analysis: dict, event: dict, news_context: List[dict]) -> str:
    tags = pick_tags(analysis, event, news_context)
    ai_text = ai_generate_market_content(analysis, event, news_context, tags)
    if ai_text:
        return ai_text
    return fallback_market_content(analysis, event, news_context, tags)




# =========================
# 币圈热词监控：RSS 证据驱动版
# 每天 10 条，分散发送，只做 RSS 中真实出现的热搜词 / 热搜币
# 不含空投撸毛，不用行情榜硬编，排除主流监控币作为热词标题
# =========================

BAD_HOTWORD_PHRASES = [
    "市场注意力正在向这个关键词集中",
    "当前监控热搜币",
    "这类热词通常会影响搜索曝光",
    "近期在币圈监控、行情监控",
    "反复出现，说明市场注意力",
    "主流币、山寨币、交易所公告或链上资金流向的搜索曝光",
]


def split_env_list(value: str) -> List[str]:
    parts = []
    for x in re.split(r"[,，\n\s]+", value or ""):
        x = x.strip()
        if x:
            parts.append(x)
    return parts


def hotword_primary_exclude_terms() -> set:
    """热词栏目主标题排除词：避免和行情监控栏目重复。"""
    base = set()

    # 用户可在 Railway 里扩展排除词
    for x in split_env_list(HOTWORD_EXTRA_EXCLUDE):
        base.add(x.upper())
        base.add(x.lower())

    # 默认排除稳定币、包装币、质押衍生资产
    for x in STABLE_SYMBOLS | EXCLUDED_TOP_SYMBOLS:
        base.add(x.upper())
        base.add(x.lower())

    # 默认排除主流监控币及常见中英文别名
    mainstream_aliases = {
        "BTC", "BITCOIN", "比特币", "比特币行情", "BTC监控",
        "ETH", "ETHEREUM", "ETHER", "以太坊", "以太坊行情", "ETH监控",
        "BNB", "BNB CHAIN", "币安币", "币安链",
        "XRP", "RIPPLE",
        "SOL", "SOLANA", "SOL监控",
        "TRX", "TRON", "波场",
        "DOGE", "DOGECOIN", "狗狗币",
        "ADA", "CARDANO", "艾达币",
        "LINK", "CHAINLINK",
        "AVAX", "AVALANCHE",
    }
    for x in mainstream_aliases:
        base.add(x.upper())
        base.add(x.lower())
        base.add(x)

    if HOTWORD_EXCLUDE_MONITORED_COINS:
        for symbol, display in SYMBOLS.items():
            base.add(display.upper())
            base.add(display.lower())
            base.add(display)
            base.add(symbol.replace("USDT", "").upper())

    return base


def is_hotword_primary_blocked(term: str) -> bool:
    if not term:
        return True
    raw = term.strip()
    upper = raw.upper()
    lower = raw.lower()

    if hotword_contains_blocked(raw):
        return True

    excludes = hotword_primary_exclude_terms()
    if raw in excludes or upper in excludes or lower in excludes:
        return True

    # 形如 BTC监控 / ETH行情 也排除
    normalized = re.sub(r"(行情|监控|价格|走势|热搜|币圈|主流币)", "", raw, flags=re.I).strip()
    if normalized and (normalized in excludes or normalized.upper() in excludes or normalized.lower() in excludes):
        return True

    return False


def hotword_contains_blocked(text: str) -> bool:
    lower = (text or "").lower()
    if any(k in lower for k in HOTWORD_BLOCK_KEYWORDS):
        return True
    if any(k in lower for k in STABLECOIN_NEWS_KEYWORDS):
        return True
    return False


def hotword_low_quality(text: str) -> bool:
    if not text:
        return True
    return any(p in text for p in BAD_HOTWORD_PHRASES)


def hotword_term_in_text(pattern: str, text_lower: str) -> bool:
    p = pattern.lower().strip()
    if not p:
        return False
    # 英文/数字词用边界，避免 sol 命中 solution
    if re.fullmatch(r"[a-z0-9][a-z0-9+.-]*", p):
        return re.search(rf"(?<![a-z0-9]){re.escape(p)}(?![a-z0-9])", text_lower) is not None
    return p in text_lower


HOTWORD_RSS_KEYWORD_RULES = [
    # 板块与叙事
    ("meme", "MEME", "板块热词"), ("memecoin", "MEME", "板块热词"), ("memecoins", "MEME", "板块热词"),
    ("ai token", "AI币", "板块热词"), ("ai tokens", "AI币", "板块热词"), ("artificial intelligence", "AI币", "板块热词"),
    ("rwa", "RWA", "板块热词"), ("real world asset", "RWA", "板块热词"), ("tokenization", "代币化资产", "板块热词"),
    ("depin", "DePIN", "板块热词"), ("gamefi", "GameFi", "板块热词"), ("gaming", "GameFi", "板块热词"),
    ("layer 2", "Layer2", "板块热词"), ("l2", "Layer2", "板块热词"),
    ("restaking", "Restaking", "板块热词"), ("staking", "质押", "链上热词"),
    ("ordinals", "Ordinals", "板块热词"), ("runes", "Runes", "板块热词"),
    ("nft", "NFT", "板块热词"), ("defi", "DeFi", "板块热词"), ("dex", "DEX", "板块热词"),
    ("perp", "Perp", "合约热词"), ("perpetual", "Perp", "合约热词"),

    # 交易所与公告
    ("binance", "Binance", "交易所热词"), ("okx", "OKX", "交易所热词"), ("coinbase", "Coinbase", "交易所热词"),
    ("kraken", "Kraken", "交易所热词"), ("bybit", "Bybit", "交易所热词"),
    ("listing", "新币上线", "交易所热词"), ("listed", "新币上线", "交易所热词"),
    ("launchpool", "Launchpool", "交易所热词"), ("launchpad", "Launchpad", "交易所热词"),
    ("exchange", "交易所公告", "交易所热词"),

    # 宏观监管
    ("sec", "SEC", "监管热词"), ("regulation", "监管", "监管热词"), ("regulatory", "监管", "监管热词"),
    ("etf", "ETF", "宏观热词"), ("filing", "ETF文件", "宏观热词"),
    ("fed", "美联储", "宏观热词"), ("federal reserve", "美联储", "宏观热词"), ("rate cut", "降息", "宏观热词"),
    ("inflation", "通胀", "宏观热词"), ("cpi", "CPI", "宏观热词"),

    # 链上与风险
    ("whale", "巨鲸", "链上热词"), ("wallet", "钱包监控", "链上热词"),
    ("on-chain", "链上监控", "链上热词"), ("onchain", "链上监控", "链上热词"),
    ("inflow", "资金流向", "链上热词"), ("outflow", "资金流向", "链上热词"),
    ("liquidation", "爆仓", "合约热词"), ("liquidations", "爆仓", "合约热词"),
    ("futures", "合约", "合约热词"), ("open interest", "持仓量", "合约热词"), ("funding rate", "资金费率", "合约热词"),
    ("hack", "黑客攻击", "风险热词"), ("exploit", "漏洞攻击", "风险热词"),

    # RSS 中出现的非主流热搜币，可作为热币栏目
    ("ton", "TON", "热搜币"), ("near", "NEAR", "热搜币"), ("apt", "APT", "热搜币"), ("aptos", "APT", "热搜币"),
    ("sui", "SUI", "热搜币"), ("wld", "WLD", "热搜币"), ("worldcoin", "WLD", "热搜币"),
    ("fet", "FET", "热搜币"), ("fetch.ai", "FET", "热搜币"), ("inj", "INJ", "热搜币"), ("injective", "INJ", "热搜币"),
    ("tia", "TIA", "热搜币"), ("celestia", "TIA", "热搜币"), ("jup", "JUP", "热搜币"), ("jupiter", "JUP", "热搜币"),
    ("pyth", "PYTH", "热搜币"), ("pendle", "PENDLE", "热搜币"), ("arb", "ARB", "热搜币"), ("arbitrum", "ARB", "热搜币"),
    ("op", "OP", "热搜币"), ("optimism", "OP", "热搜币"), ("sei", "SEI", "热搜币"),
    ("render", "RENDER", "热搜币"), ("rndr", "RENDER", "热搜币"), ("tao", "TAO", "热搜币"), ("bittensor", "TAO", "热搜币"),
    ("kas", "KAS", "热搜币"), ("kaspa", "KAS", "热搜币"), ("fil", "FIL", "热搜币"), ("filecoin", "FIL", "热搜币"),
    ("ltc", "LTC", "热搜币"), ("litecoin", "LTC", "热搜币"), ("bch", "BCH", "热搜币"), ("bitcoin cash", "BCH", "热搜币"),
    ("zec", "ZEC", "热搜币"), ("zcash", "ZEC", "热搜币"), ("dot", "DOT", "热搜币"), ("polkadot", "DOT", "热搜币"),
    ("uni", "UNI", "热搜币"), ("uniswap", "UNI", "热搜币"), ("aave", "AAVE", "热搜币"),
    ("ondo", "ONDO", "热搜币"), ("ena", "ENA", "热搜币"), ("hype", "HYPE", "热搜币"),
]


def hotword_tags_for_term(term: str, category: str = "") -> str:
    candidates = []
    t = (term or "").upper()

    if category == "交易所热词" or any(x in t for x in ["BINANCE", "OKX", "COINBASE", "KRAKEN", "BYBIT"]):
        candidates.extend(["#交易所监控", "#新币上线", "#币圈热词", "#行情监控"])
    elif category == "合约热词" or term in {"爆仓", "合约", "持仓量", "资金费率", "Perp"}:
        candidates.extend(["#爆仓监控", "#合约", "#多空变化", "#币圈热词"])
    elif category == "链上热词":
        candidates.extend(["#链上监控", "#巨鲸监控", "#资金流向", "#币圈热词"])
    elif category in {"监管热词", "宏观热词"}:
        candidates.extend(["#SEC", "#ETF", "#监管", "#币圈热词", "#宏观"])
    elif category == "热搜币":
        candidates.extend([f"#{term}", "#热搜币", "#山寨币", "#币圈热词", "#行情监控"])
    else:
        candidates.extend([f"#{term}" if re.fullmatch(r"[A-Za-z0-9]+", term or "") else "#币圈热词", "#热词监控", "#行情监控", "#山寨币"])

    pool = [x for x in HOTWORD_TAG_POOL if not is_hotword_primary_blocked(x.replace("#", ""))]
    random.shuffle(pool)
    candidates.extend(pool)

    candidates = [x for x in unique_keep_order(candidates) if not hotword_contains_blocked(x)]
    candidates = [x for x in candidates if not is_hotword_primary_blocked(x.replace("#", ""))]
    target_count = random.randint(3, 5)
    return " ".join(candidates[:target_count])


def summarize_evidence(evidence: List[dict], max_items: int = 3) -> str:
    if not evidence:
        return "RSS 新闻标题和摘要出现相关内容。"
    sources = unique_keep_order([e.get("source", "RSS") for e in evidence if e.get("source")])[:max_items]
    if not sources:
        sources = ["RSS"]
    return "、".join(sources) + " 的 RSS 标题/摘要出现相关内容"


def add_hotword_score(scores: dict, term: str, points: float, category: str, evidence: dict):
    term = clean_html(term).strip()
    if not term or is_hotword_primary_blocked(term):
        return
    if len(term) > 24:
        return

    item = scores.setdefault(term, {
        "term": term,
        "category": category or "热词",
        "score": 0.0,
        "evidence": [],
        "related_terms": set(),
        "sources": set(),
    })
    item["score"] += points
    if category and not item.get("category"):
        item["category"] = category

    if evidence:
        ev_key = (evidence.get("source"), evidence.get("title"))
        exists = any((e.get("source"), e.get("title")) == ev_key for e in item["evidence"])
        if not exists and len(item["evidence"]) < HOTWORD_MAX_EVIDENCE_PER_TERM:
            item["evidence"].append(evidence)
        if evidence.get("source"):
            item["sources"].add(evidence.get("source"))


def collect_hotword_candidates() -> List[dict]:
    """只从已入库 RSS 新闻中提取热词，不用行情榜和内置词硬凑。"""
    # 确保热词队列生成前先刷新一次 RSS
    try:
        update_news_cache(force=True)
    except Exception as e:
        print("热词生成前刷新 RSS 失败:", e)

    cutoff = time.time() - NEWS_LOOKBACK_HOURS * 3600
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT source, title, summary, categories, symbols, published_at
        FROM news_items
        WHERE published_at >= ?
        ORDER BY published_at DESC
        LIMIT 220
    """, (cutoff,))
    rows = cur.fetchall()
    conn.close()

    scores = {}

    for source, title, summary, categories_raw, symbols_raw, published_at in rows:
        title = clean_html(title or "")
        summary = clean_html(summary or "")
        text = f"{title} {summary}"
        lower = text.lower()

        if not title or hotword_contains_blocked(text):
            continue

        try:
            categories = json.loads(categories_raw or "[]")
        except Exception:
            categories = []
        if "airdrop" in categories:
            continue

        evidence = {
            "source": source or "RSS",
            "title": short_text(title, 120),
            "summary": short_text(summary, 180),
            "categories": categories,
            "published_at": published_at,
        }

        # 时间越新，权重略高
        age_hours = max((time.time() - (published_at or time.time())) / 3600, 0)
        recency = max(0.6, 1.2 - age_hours / max(NEWS_LOOKBACK_HOURS, 1))

        for pattern, canonical, category in HOTWORD_RSS_KEYWORD_RULES:
            if is_hotword_primary_blocked(canonical):
                continue
            if hotword_term_in_text(pattern, lower):
                add_hotword_score(scores, canonical, 3.0 * recency, category, evidence)

    items = []
    for item in scores.values():
        evidence = item.get("evidence", [])
        if len(evidence) < HOTWORD_MIN_RSS_EVIDENCE:
            continue
        item["sources"] = list(item.get("sources", []))
        item["related_terms"] = []
        # 多源加分、重复出现加分
        item["score"] += min(len(evidence), 5) * 1.5
        item["score"] += min(len(item["sources"]), 4) * 1.0
        items.append(item)

    items.sort(key=lambda x: (x.get("score", 0), len(x.get("sources", [])), len(x.get("evidence", []))), reverse=True)

    final = []
    used = set()
    for item in items:
        term = item["term"]
        key = term.upper()
        if key in used:
            continue
        used.add(key)
        final.append(item)
        if len(final) >= max(HOTWORD_POSTS_PER_DAY * 2, 20):
            break

    print("RSS 热词候选：", [(x["term"], round(x["score"], 2), len(x.get("evidence", []))) for x in final[:12]])
    return final


def category_reason(term: str, category: str) -> str:
    if category == "交易所热词":
        return "交易所公告、上新、Launchpool 或活动信息会直接影响项目热度，也容易带动山寨币资金流向。"
    if category == "合约热词":
        return "合约、爆仓、持仓量和资金费率通常代表短线交易情绪，容易放大行情波动。"
    if category == "链上热词":
        return "链上资金、巨鲸钱包和交易所流入流出能帮助判断资金是否正在移动。"
    if category in {"监管热词", "宏观热词"}:
        return "监管、ETF、利率和宏观消息会影响市场风险偏好，也会改变主流资金的交易节奏。"
    if category == "热搜币":
        return f"{term} 出现在 RSS 新闻里，说明它具备独立事件或板块关注度，适合放进热搜币观察。"
    if category == "板块热词":
        return "板块叙事一旦被多家新闻源提到，通常会影响同类山寨币和相关项目的搜索热度。"
    return "该词来自 RSS 新闻标题/摘要，属于当前币圈消息面里可观察的关键词。"


def related_keywords_for_hotword(term: str, category: str) -> List[str]:
    mapping = {
        "交易所热词": [term, "交易所公告", "新币上线", "Launchpool", "山寨币"],
        "合约热词": [term, "合约情绪", "爆仓监控", "多空变化", "短线波动"],
        "链上热词": [term, "链上监控", "巨鲸", "资金流向", "交易所流入"],
        "监管热词": [term, "SEC", "ETF", "监管", "交易所合规"],
        "宏观热词": [term, "ETF", "美联储", "利率", "宏观监管"],
        "热搜币": [term, "热搜币", "山寨币", "行情监控", "交易所动态"],
        "板块热词": [term, "山寨币", "板块轮动", "资金情绪", "行情监控"],
        "风险热词": [term, "风险监控", "链上安全", "交易所公告", "资金流向"],
    }
    kws = mapping.get(category, [term, "币圈热词", "行情监控", "市场消息", "交易所动态"])
    kws = [x for x in unique_keep_order(kws) if x and not is_hotword_primary_blocked(x)]
    return kws[:5]


def build_hotword_fallback_post(item: dict, slot_no: int) -> dict:
    term = item.get("term", "币圈热词")
    category = item.get("category", "热词")
    evidence = item.get("evidence", [])
    tags = hotword_tags_for_term(term, category)
    source_line = summarize_evidence(evidence)
    related = "、".join(related_keywords_for_hotword(term, category)[:5])
    reason = category_reason(term, category)

    title_term = short_text(term, 12)
    content = f"""【币圈热词监控｜{title_term}】

热词来源：
{source_line}，所以“{term}”被纳入本轮 RSS 热词候选。

为什么值得看：
{reason}

观察方向：
后续重点看这个词是否继续出现在交易所公告、宏观监管、链上资金或山寨板块新闻里，避免只看单条消息。

相关关键词：
{related}

{tags}""".strip()

    return {
        "slot_no": slot_no,
        "title": f"币圈热词监控｜{title_term}",
        "hotword": term,
        "content": content,
    }


def extract_json_array(text: str):
    if not text:
        return None
    m = re.search(r"\[.*\]", text, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


def ai_generate_hotword_posts(candidates: List[dict]) -> Optional[List[dict]]:
    if not USE_AI_POLISH or not client:
        return None
    if not candidates:
        return None

    compact = []
    for item in candidates[:25]:
        compact.append({
            "term": item.get("term"),
            "category": item.get("category"),
            "score": round(item.get("score", 0), 2),
            "sources": item.get("sources", [])[:4],
            "evidence": [
                {
                    "source": e.get("source"),
                    "title": e.get("title"),
                    "summary": e.get("summary"),
                }
                for e in item.get("evidence", [])[:3]
            ],
        })

    max_count = min(HOTWORD_POSTS_PER_DAY, len(compact))
    prompt = f"""
你是 Telegram 币圈频道编辑。请根据下面 RSS 热词证据，生成 {max_count} 条“币圈热词监控”内容。

硬性原则：
1. 只能根据给定 RSS 标题/摘要证据写，不允许编造新闻、数据、项目进展。
2. 不要写 BTC、ETH、BNB、XRP、SOL、TRX、DOGE、ADA、LINK、AVAX 作为热词标题。
3. 不写空投、撸毛、测试网、积分任务、白名单、Galxe、Layer3、Zealy。
4. 不写 USDT、USDC、DAI、稳定币。
5. 禁止空话：不要写“市场注意力正在向这个关键词集中”“当前监控热搜币”“影响搜索曝光”“近期在币圈监控、行情监控里反复出现”。
6. 每条必须有依据，必须出现“热词来源：”，并点明来自 RSS 标题/摘要或来源名称。
7. 每条必须包含：热词来源、为什么值得看、观察方向、相关关键词。
8. 每条最后一行 3-5 个标签，最多5个。
9. 不要输出链接，不要输出英文长段，不要投资建议。

固定格式：
【币圈热词监控｜关键词】

热词来源：
说明 RSS 证据来自哪些来源/标题摘要，不要照抄过长标题。

为什么值得看：
结合该热词对应的板块、交易所、合约、链上或监管影响，写具体点。

观察方向：
写接下来应该看什么消息是否继续发酵。

相关关键词：
3-5个，用顿号分隔。

#标签1 #标签2 #标签3

输出 JSON 数组，不要输出 JSON 以外内容：
[
  {{"title":"币圈热词监控｜SEC","hotword":"SEC","content":"完整频道文案"}}
]

RSS 热词证据：
{json.dumps(compact, ensure_ascii=False)}
""".strip()

    try:
        response = client.responses.create(model=MODEL_NAME, input=prompt)
        raw = (response.output_text or "").strip()
        arr = extract_json_array(raw)
        if not isinstance(arr, list):
            return None

        result = []
        allowed_terms = {x.get("term") for x in candidates}
        for idx, obj in enumerate(arr[:max_count], 1):
            if not isinstance(obj, dict):
                continue
            title = clean_html(str(obj.get("title", ""))).strip()
            hotword = clean_html(str(obj.get("hotword", ""))).strip()
            content = str(obj.get("content", "")).strip().replace("...", "").replace("……", "")
            content = re.sub(r"\n{3,}", "\n\n", content)

            if not title or not hotword or not content:
                continue
            if is_hotword_primary_blocked(hotword) or is_hotword_primary_blocked(title.replace("币圈热词监控｜", "")):
                continue
            if hotword_contains_blocked(content) or hotword_low_quality(content):
                continue
            if "热词来源" not in content:
                continue
            if hotword not in allowed_terms and title.replace("币圈热词监控｜", "") not in allowed_terms:
                # AI 如果新造了候选外热词，跳过
                continue
            if "【币圈热词监控" not in content:
                content = f"【{title}】\n\n{content}"

            # 标签最多5个
            parts = content.splitlines()
            if parts:
                last = parts[-1].strip()
                tags = [x for x in last.split() if x.startswith("#")]
                if len(tags) > 5:
                    parts[-1] = " ".join(tags[:5])
                    content = "\n".join(parts)

            result.append({"slot_no": idx, "title": title, "hotword": hotword, "content": content})

        return result if result else None

    except Exception as e:
        print("AI 生成 RSS 热词栏目失败，使用证据模板:", e)
        return None


def build_hotword_posts() -> List[dict]:
    candidates = collect_hotword_candidates()
    if not candidates:
        print("没有足够 RSS 证据生成热词栏目，本日跳过热词队列")
        return []

    posts = ai_generate_hotword_posts(candidates)
    if posts:
        # AI 不足时，用证据模板补齐，但只用 RSS 有证据的候选
        if len(posts) < min(HOTWORD_POSTS_PER_DAY, len(candidates)):
            used = {p.get("hotword") for p in posts}
            slot = len(posts) + 1
            for item in candidates:
                if slot > HOTWORD_POSTS_PER_DAY:
                    break
                if item.get("term") in used:
                    continue
                posts.append(build_hotword_fallback_post(item, slot))
                slot += 1
        return posts[:HOTWORD_POSTS_PER_DAY]

    fallback = []
    for idx, item in enumerate(candidates[:HOTWORD_POSTS_PER_DAY], 1):
        fallback.append(build_hotword_fallback_post(item, idx))
    return fallback[:HOTWORD_POSTS_PER_DAY]


def hotword_queue_count(date_key: str) -> int:
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM hotword_queue WHERE date_key = ?", (date_key,))
    count = cur.fetchone()[0]
    conn.close()
    return count


def generate_hotword_queue_if_needed(force: bool = False):
    if not ENABLE_HOTWORD_COLUMN:
        return

    now = datetime.now()
    date_key = current_date_key()

    if not force and now.hour < HOTWORD_GENERATE_HOUR:
        return

    if hotword_queue_count(date_key) >= HOTWORD_POSTS_PER_DAY:
        return

    posts = build_hotword_posts()
    times = hotword_send_times()

    conn = sqlite3.connect("data.db")
    cur = conn.cursor()

    for idx, post in enumerate(posts[:HOTWORD_POSTS_PER_DAY], 1):
        send_time = times[idx - 1] if idx - 1 < len(times) else times[-1]
        cur.execute("""
            INSERT OR IGNORE INTO hotword_queue(
                date_key, slot_no, send_time, title, hotword, content, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
        """, (
            date_key,
            idx,
            send_time,
            post.get("title", ""),
            post.get("hotword", ""),
            post.get("content", ""),
            time.time(),
        ))

    conn.commit()
    conn.close()
    print(f"币圈热词监控队列已生成：{min(len(posts), HOTWORD_POSTS_PER_DAY)} 条")


def fetch_due_hotword_post() -> Optional[dict]:
    if not ENABLE_HOTWORD_COLUMN:
        return None

    date_key = current_date_key()
    now_hhmm = datetime.now().strftime("%H:%M")

    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT id, slot_no, send_time, title, hotword, content
        FROM hotword_queue
        WHERE date_key = ?
          AND status = 'pending'
          AND send_time <= ?
        ORDER BY slot_no ASC
        LIMIT 1
    """, (date_key, now_hhmm))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "id": row[0],
        "slot_no": row[1],
        "send_time": row[2],
        "title": row[3],
        "hotword": row[4],
        "content": row[5],
    }


def mark_hotword_sent(row_id: int):
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        UPDATE hotword_queue
        SET status = 'sent', sent_at = ?
        WHERE id = ?
    """, (time.time(), row_id))
    conn.commit()
    conn.close()


def get_hotword_image_path() -> str:
    path = image_path(HOTWORD_IMAGE)
    if os.path.isfile(path):
        return path

    fallback = image_path(FALLBACK_MARKET_IMAGE)
    if os.path.isfile(fallback):
        return fallback

    return ""


def process_hotword_column() -> bool:
    if not ENABLE_HOTWORD_COLUMN:
        return False

    generate_hotword_queue_if_needed(force=False)

    row = fetch_due_hotword_post()
    if not row:
        return False

    if total_posts_today() >= MAX_POSTS_PER_DAY or total_posts_last_hour() >= MAX_POSTS_PER_HOUR:
        print("币圈热词监控因全局上限跳过，稍后重试")
        return False

    if not global_cooldown_ok():
        print("币圈热词监控因全频道冷却跳过，稍后重试")
        return False

    content = row["content"]
    if hotword_contains_blocked(content):
        print("热词内容命中禁止词，跳过并标记已发送避免重复:", row.get("title"))
        mark_hotword_sent(row["id"])
        return False

    image = get_hotword_image_path()
    resp = send_with_image(content, image)

    if resp.status_code == 200:
        now = time.time()
        set_meta("last_global_sent_at", str(now))
        record_sent("hotword", "", "hotword", title=row.get("title", "")[:80])
        mark_hotword_sent(row["id"])
        print(f"币圈热词监控已发送：{row.get('send_time')} {row.get('title')}")
        return True

    print("币圈热词监控发送失败")
    return False



# =========================
# 空投 / 撸毛内容
# =========================

LM_GENERIC_TITLES = [
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


def pick_lm_tags() -> str:
    target_count = random.randint(3, 5)
    pool = LM_TAG_POOL[:]
    random.shuffle(pool)
    return " ".join(unique_keep_order(pool[:target_count])[:5])


def ai_generate_airdrop_content(news: Optional[dict], tags: str) -> Optional[str]:
    if not USE_AI_POLISH or not client:
        return None

    if news:
        title = news.get("title", "")
        summary = news.get("summary", "")
        prompt = f"""
请把下面空投/撸毛资讯整理成 Telegram 频道文案。

要求：
1. 标题格式：【空投监控｜任务更新】或【撸毛情报｜任务观察】
2. 不要输出任何链接
3. 必须包含：在哪撸、要求、能撸到什么
4. 可以提示去哪里找，比如 Galxe、Layer3、Zealy、项目官网任务中心、官方 Discord、官方 X 置顶
5. 不要承诺一定空投，不要写稳赚、必拿、确定发币
6. 尽量自然带入：空投监控、撸毛情报、测试网交互、积分任务、钱包交互
7. 最后一行必须原样使用这些标签：{tags}
8. 控制在 650 个中文字以内

标题：{title}
摘要：{summary}

请直接输出频道文案。
""".strip()
    else:
        focus = "、".join(random.choice(LM_FOCUS_POOL))
        prompt = f"""
请生成一条 Telegram 币圈频道的撸毛/空投栏目内容。

要求：
1. 标题格式：【{random.choice(LM_GENERIC_TITLES)}】
2. 主题围绕：{focus}
3. 必须包含：关注方向、适合人群、监控重点
4. 不要编造具体项目，不要输出链接
5. 自然带入关键词：空投监控、撸毛情报、测试网交互、积分任务、钱包交互、Galxe、Layer3、Zealy
6. 最后一行必须原样使用这些标签：{tags}
7. 控制在 600 个中文字以内

请直接输出频道文案。
""".strip()

    try:
        response = client.responses.create(model=MODEL_NAME, input=prompt)
        text = (response.output_text or "").strip()
        text = text.replace("...", "").replace("……", "")
        if 50 <= len(text) <= 1100:
            return text
    except Exception as e:
        print("AI 生成撸毛文案失败，使用备用模板:", e)

    return None


def fallback_airdrop_content(news: Optional[dict], tags: str) -> str:
    if news:
        title = news.get("title", "空投任务更新")
        summary = short_text(news.get("summary", ""), 220)
        summary_line = summary if summary else "当前信息有限，具体规则以官方任务页为准。"
        return f"""【空投监控｜任务更新】

资讯摘要：
{title}

在哪撸：
可优先去项目官网任务中心、Galxe、Layer3、Zealy、官方 Discord 或官方 X 置顶内容里搜索项目名和任务入口。

要求：
重点看钱包交互、测试网操作、社媒绑定、积分任务、白名单或徽章任务。{summary_line}

能撸到什么：
可能是积分、徽章、NFT、白名单、测试网奖励或潜在空投资格，具体规则以官方任务页为准。

{tags}""".strip()

    focus = "、".join(random.choice(LM_FOCUS_POOL))
    title = random.choice(LM_GENERIC_TITLES)
    return f"""【{title}】

今天的空投监控可以重点看测试网交互、积分任务和项目官网任务中心。撸毛情报不是盲目冲，重点是筛选有任务体系、有交互记录、有持续更新的项目。

关注方向：
{focus}

适合人群：
低成本交互、长期撸毛、早期项目观察。

监控重点：
钱包交互成本、积分记录、官方任务页、Galxe、Layer3、Zealy、官方 Discord 和官方 X 置顶内容。

{tags}""".strip()


def build_airdrop_message(news: Optional[dict]) -> str:
    tags = pick_lm_tags()
    ai_text = ai_generate_airdrop_content(news, tags)
    if ai_text:
        return ai_text
    return fallback_airdrop_content(news, tags)



# =========================
# 扩展栏目：交易所监控 / 合约情绪 / 山寨板块雷达 / 币圈小词典
# 新增栏目合计每天 3-5 条，不占用原有栏目频率
# =========================

EXTRA_COLUMN_META = {
    "exchange": {
        "name": "交易所监控",
        "image": EXCHANGE_IMAGE,
        "tags": ["#交易所监控", "#Binance", "#OKX", "#新币上线", "#Launchpool", "#交易所公告"],
        "keywords": ["Binance", "OKX", "Coinbase", "新币上线", "Launchpool", "Launchpad", "交易所公告", "上新监控"],
    },
    "contract": {
        "name": "合约情绪监控",
        "image": CONTRACT_IMAGE,
        "tags": ["#合约", "#爆仓监控", "#多空变化", "#洗盘", "#资金费率", "#行情异动"],
        "keywords": ["爆仓", "合约", "多空", "洗盘", "杠杆情绪", "资金费率", "持仓量", "短线波动"],
    },
    "sector": {
        "name": "山寨板块雷达",
        "image": SECTOR_IMAGE,
        "tags": ["#山寨雷达", "#山寨币", "#MEME", "#AI币", "#RWA", "#板块轮动"],
        "keywords": ["山寨币", "MEME", "AI币", "RWA", "DePIN", "GameFi", "Layer2", "Restaking", "公链生态"],
    },
    "wiki": {
        "name": "币圈小词典",
        "image": WIKI_IMAGE,
        "tags": ["#币圈小词典", "#新手科普", "#币圈监控", "#行情监控", "#资金安全"],
        "keywords": ["合约爆仓", "资金费率", "Launchpool", "新币上线", "测试网交互", "空投", "支撑压力", "链上监控"],
    },
}

EXTRA_COLUMN_KEYWORD_RULES = {
    "exchange": [
        "binance", "okx", "coinbase", "kraken", "bybit", "listing", "listed", "launchpool", "launchpad",
        "exchange", "new token", "new listing", "交易所", "上新", "新币上线", "公告",
    ],
    "contract": [
        "liquidation", "liquidations", "futures", "perpetual", "open interest", "funding rate", "leverage",
        "longs", "shorts", "爆仓", "合约", "多空", "洗盘", "资金费率", "持仓量", "杠杆",
    ],
    "sector": [
        "meme", "memecoin", "memecoins", "ai token", "ai tokens", "rwa", "depin", "gamefi", "layer 2",
        "restaking", "altcoin", "altcoins", "solana ecosystem", "defi", "nft", "山寨", "山寨币", "板块", "公链",
    ],
}

WIKI_TOPICS = [
    {
        "title": "币圈小词典｜合约爆仓",
        "body": "合约爆仓不是单纯亏损，而是保证金不足时仓位被强制平掉。币圈里看到爆仓、合约、多空、洗盘这些词一起出现，通常说明短线杠杆情绪升温，行情更容易出现急拉急跌。",
        "keywords": "合约爆仓、杠杆情绪、多空变化、洗盘、风险监控",
        "tags": "#合约爆仓 #币圈小词典 #多空变化 #风险监控",
    },
    {
        "title": "币圈小词典｜资金费率",
        "body": "资金费率可以理解为合约多空双方的情绪温度。费率持续偏高，说明追多情绪偏热；费率持续偏低，说明空头更拥挤。它不能单独判断涨跌，但适合配合持仓量、成交量和支撑压力一起看。",
        "keywords": "资金费率、合约情绪、持仓量、多空变化、行情监控",
        "tags": "#资金费率 #合约 #币圈小词典 #行情监控",
    },
    {
        "title": "币圈小词典｜Launchpool",
        "body": "Launchpool 常见于交易所新币活动，用户通过质押指定资产参与新币分发。它本身不等于项目一定上涨，但会带来新币上线、交易所公告、活动任务和山寨币热度。",
        "keywords": "Launchpool、新币上线、交易所公告、Binance、上新监控",
        "tags": "#Launchpool #新币上线 #交易所监控 #币圈小词典",
    },
    {
        "title": "币圈小词典｜支撑压力",
        "body": "支撑压力是短线行情监控里最常用的观察点。支撑附近看承接，压力附近看放量突破。单看一个价位意义不大，更重要的是成交量、K线结构和市场情绪是否配合。",
        "keywords": "支撑压力、行情监控、短线结构、成交量、盘面观察",
        "tags": "#支撑压力 #行情监控 #短线结构 #币圈小词典",
    },
    {
        "title": "币圈小词典｜链上监控",
        "body": "链上监控主要看钱包地址、交易所流入流出、巨鲸转账和资金流向。它不一定马上反映到价格上，但经常能帮助判断资金是在进场、撤离，还是准备制造短线波动。",
        "keywords": "链上监控、巨鲸监控、资金流向、钱包监控、交易所流入",
        "tags": "#链上监控 #巨鲸监控 #资金流向 #币圈小词典",
    },
    {
        "title": "币圈小词典｜测试网交互",
        "body": "测试网交互通常出现在空投和早期项目任务里，比如领水、转账、Swap、Mint、做任务验证等。重点不是乱点链接，而是看项目官网、任务入口、钱包交互成本和积分记录是否清晰。",
        "keywords": "测试网交互、空投、撸毛、积分任务、钱包交互",
        "tags": "#测试网交互 #空投 #撸毛 #币圈小词典",
    },
]


def extra_column_enabled() -> bool:
    return ENABLE_EXTRA_COLUMNS and EXTRA_COLUMN_POSTS_PER_DAY > 0


def extra_column_image_path(column_type: str) -> str:
    meta = EXTRA_COLUMN_META.get(column_type, {})
    filename = meta.get("image") or FALLBACK_MARKET_IMAGE
    path = image_path(filename)
    if os.path.isfile(path):
        return path

    fallback = image_path(FALLBACK_MARKET_IMAGE)
    if os.path.isfile(fallback):
        return fallback

    return ""


def pick_extra_tags(column_type: str) -> str:
    meta = EXTRA_COLUMN_META.get(column_type, {})
    base = meta.get("tags", [])[:]
    random.shuffle(base)
    target = random.randint(3, 5)
    return " ".join(unique_keep_order(base)[:target])


def extra_column_queue_count(date_key: str) -> int:
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM extra_column_queue WHERE date_key = ?", (date_key,))
    count = cur.fetchone()[0]
    conn.close()
    return count


def fetch_extra_column_evidence(column_type: str, limit: int = 3) -> List[dict]:
    if column_type == "wiki":
        return []

    cutoff = time.time() - NEWS_LOOKBACK_HOURS * 3600
    rules = EXTRA_COLUMN_KEYWORD_RULES.get(column_type, [])
    if not rules:
        return []

    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT link, source, title, summary, categories, symbols, used_count, published_at
        FROM news_items
        WHERE published_at >= ?
        ORDER BY used_count ASC, published_at DESC
        LIMIT 120
    """, (cutoff,))
    rows = cur.fetchall()
    conn.close()

    scored = []
    for row in rows:
        link, source, title, summary, categories_raw, symbols_raw, used_count, published_at = row
        text = f"{title} {summary}".lower()
        if hotword_contains_blocked(text) and column_type != "sector":
            continue

        score = 0
        for rule in rules:
            if hotword_term_in_text(rule, text):
                score += 2

        try:
            categories = json.loads(categories_raw or "[]")
        except Exception:
            categories = []

        if column_type == "exchange" and "exchange" in categories:
            score += 3
        elif column_type == "contract" and ("onchain" in categories or "macro" in categories):
            score += 1
        elif column_type == "sector" and "altcoin" in categories:
            score += 3

        if score > 0:
            scored.append({
                "link": link,
                "source": source,
                "title": title,
                "summary": summary,
                "categories": categories,
                "used_count": used_count,
                "published_at": published_at,
                "score": score,
            })

    scored.sort(key=lambda x: (x["score"], -x["used_count"], x["published_at"]), reverse=True)
    return scored[:limit]


def evidence_line(evidence: List[dict]) -> str:
    if not evidence:
        return "当前没有强新闻触发，主要按栏目关键词和盘面关注度做观察。"

    parts = []
    for n in evidence[:2]:
        source = n.get("source", "RSS")
        title = clean_html(n.get("title", ""))
        if title:
            parts.append(f"{source} 提到：{title}")
    return "；".join(parts) if parts else "RSS 新闻出现相关线索，适合继续观察。"


def ai_generate_extra_column_content(column_type: str, evidence: List[dict], tags: str) -> Optional[str]:
    if not USE_AI_POLISH or not client:
        return None

    meta = EXTRA_COLUMN_META.get(column_type, {})
    name = meta.get("name", "币圈观察")
    keywords = "、".join(meta.get("keywords", [])[:8])
    ev = evidence_line(evidence)

    prompt = f"""
请生成一条 Telegram 币圈频道栏目内容。

栏目：{name}
栏目关键词：{keywords}
消息依据：{ev}
标签：{tags}

固定格式：
【{name}｜短标题】

快讯观察：2句，写得像币圈资讯，不要像教程广告。
盘面重点：1句，说明为什么值得看。
相关关键词：列出4-6个中文关键词，用顿号分隔。

{tags}

硬性要求：
1. 总字数控制在 180-320 个中文字符
2. 不要喊单，不要投资建议，不要承诺涨跌
3. 没有明确消息依据时，不要伪装成真实突发新闻，只写“观察”“可关注”“关键词方向”
4. 正文自然包含栏目关键词，不要堆砌
5. 最后一行必须原样使用这些标签：{tags}
""".strip()

    try:
        response = client.responses.create(model=MODEL_NAME, input=prompt)
        text = (response.output_text or "").strip()
        text = text.replace("...", "").replace("……", "")
        text = re.sub(r"\n{3,}", "\n\n", text)
        if 80 <= len(text) <= 900:
            return text
    except Exception as e:
        print("AI 生成扩展栏目失败，使用备用模板:", e)

    return None


def fallback_extra_column_content(column_type: str, evidence: List[dict], tags: str, slot_no: int) -> str:
    if column_type == "wiki":
        topic = WIKI_TOPICS[(slot_no - 1) % len(WIKI_TOPICS)]
        return f"""【{topic['title']}】

简单说：
{topic['body']}

相关关键词：
{topic['keywords']}

{topic['tags']}""".strip()

    ev = evidence_line(evidence)

    if column_type == "exchange":
        return f"""【交易所监控｜上新观察】

快讯观察：
交易所上新、Launchpool、Launchpad 和活动任务，一直是山寨币热度的重要入口。{ev}

盘面重点：
重点看 Binance、OKX、Coinbase 相关公告，是否带动同板块项目出现短线关注。

相关关键词：
Binance、OKX、新币上线、Launchpool、交易所公告

{tags}""".strip()

    if column_type == "contract":
        return f"""【合约情绪监控｜爆仓观察】

快讯观察：
爆仓、合约、多空、洗盘这些词一旦集中出现，通常说明短线杠杆情绪升温。{ev}

盘面重点：
如果 BTC 和 ETH 没有明确方向，山寨币更容易被合约情绪带动，出现快速拉升或急跌。

相关关键词：
爆仓、合约、多空、洗盘、资金费率

{tags}""".strip()

    return f"""【山寨板块雷达｜板块观察】

快讯观察：
山寨币、MEME、AI币、RWA、DePIN、Layer2 这些板块词一旦和 RSS 新闻同时出现，说明市场注意力不只停留在主流币。{ev}

盘面重点：
如果 BTC 没有明显走弱，高弹性板块更容易获得短线资金关注。

相关关键词：
山寨币、MEME、AI币、RWA、板块轮动

{tags}""".strip()


def build_extra_column_posts() -> List[dict]:
    types = enabled_extra_column_types()
    times = extra_column_send_times()
    total = min(EXTRA_COLUMN_POSTS_PER_DAY, len(times))
    if total <= 0:
        return []

    # 按日期轮换起始栏目，避免每天顺序完全一样
    day_offset = int(current_date_key()) % max(1, len(types))
    rotated = types[day_offset:] + types[:day_offset]
    posts = []

    for idx in range(total):
        column_type = rotated[idx % len(rotated)]
        tags = pick_extra_tags(column_type)
        evidence = fetch_extra_column_evidence(column_type, limit=3)
        content = ai_generate_extra_column_content(column_type, evidence, tags)
        if not content:
            content = fallback_extra_column_content(column_type, evidence, tags, idx + 1)

        image_name = EXTRA_COLUMN_META.get(column_type, {}).get("image", FALLBACK_MARKET_IMAGE)
        title_match = re.search(r"【(.+?)】", content)
        title = title_match.group(1) if title_match else EXTRA_COLUMN_META.get(column_type, {}).get("name", "扩展栏目")

        posts.append({
            "slot_no": idx + 1,
            "send_time": times[idx],
            "column_type": column_type,
            "title": title,
            "content": content,
            "image_name": image_name,
            "evidence": evidence,
        })

    return posts


def generate_extra_column_queue_if_needed(force: bool = False):
    if not extra_column_enabled():
        return

    date_key = current_date_key()
    current_count = extra_column_queue_count(date_key)
    if not force and current_count >= EXTRA_COLUMN_POSTS_PER_DAY:
        return
    if current_count >= EXTRA_COLUMN_POSTS_PER_DAY:
        return

    posts = build_extra_column_posts()
    if not posts:
        print("扩展栏目没有生成内容")
        return

    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    for post in posts:
        cur.execute("""
            INSERT OR IGNORE INTO extra_column_queue(
                date_key, slot_no, send_time, column_type, title, content, image_name, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """, (
            date_key,
            post["slot_no"],
            post["send_time"],
            post["column_type"],
            post.get("title", ""),
            post["content"],
            post.get("image_name", ""),
            time.time(),
        ))
        for n in post.get("evidence", []):
            if n.get("link"):
                mark_news_used(n["link"])
    conn.commit()
    conn.close()
    print("扩展栏目队列已生成", len(posts), "条")


def fetch_due_extra_column_post() -> Optional[dict]:
    if not extra_column_enabled():
        return None

    now = datetime.now()
    now_minutes = now.hour * 60 + now.minute
    date_key = current_date_key()

    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        SELECT id, slot_no, send_time, column_type, title, content, image_name
        FROM extra_column_queue
        WHERE date_key = ?
          AND status = 'pending'
        ORDER BY slot_no ASC
    """, (date_key,))
    rows = cur.fetchall()
    conn.close()

    for row in rows:
        send_minutes = parse_hhmm_to_minutes(row[2])
        if now_minutes >= send_minutes:
            return {
                "id": row[0],
                "slot_no": row[1],
                "send_time": row[2],
                "column_type": row[3],
                "title": row[4],
                "content": row[5],
                "image_name": row[6],
            }

    return None


def mark_extra_column_sent(row_id: int):
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.execute("""
        UPDATE extra_column_queue
        SET status = 'sent', sent_at = ?
        WHERE id = ?
    """, (time.time(), row_id))
    conn.commit()
    conn.close()


def process_extra_columns() -> bool:
    if not extra_column_enabled():
        return False

    generate_extra_column_queue_if_needed(force=False)

    if extra_column_posts_today() >= EXTRA_COLUMN_POSTS_PER_DAY:
        return False

    last = float(get_meta("last_extra_column_sent_at", "0") or 0)
    if time.time() - last < EXTRA_COLUMN_MIN_INTERVAL_SECONDS:
        return False

    row = fetch_due_extra_column_post()
    if not row:
        return False

    image = image_path(row.get("image_name") or FALLBACK_MARKET_IMAGE)
    if not os.path.isfile(image):
        image = extra_column_image_path(row.get("column_type", ""))

    resp = send_with_image(row["content"], image)
    if resp.status_code == 200:
        set_meta("last_extra_column_sent_at", str(time.time()))
        record_sent("extra_column", "", row.get("column_type", "extra"), title=row.get("title", "")[:80])
        mark_extra_column_sent(row["id"])
        print("扩展栏目已发送:", row.get("title"))
        return True

    print("扩展栏目发送失败")
    return False


# =========================
# 图片与 Telegram
# =========================

def image_path(filename: str) -> str:
    return os.path.join(IMAGES_DIR, filename)


def get_symbol_image_path(symbol: str) -> str:
    filename = DEFAULT_IMAGE_MAP.get(symbol)
    if filename and os.path.isfile(image_path(filename)):
        return image_path(filename)

    fallback = image_path(FALLBACK_MARKET_IMAGE)
    if os.path.isfile(fallback):
        return fallback

    return ""


def get_lm_image_path() -> str:
    path = image_path(LM_IMAGE)
    if os.path.isfile(path):
        return path

    fallback = image_path(FALLBACK_MARKET_IMAGE)
    if os.path.isfile(fallback):
        return fallback

    return ""


def safe_caption(text: str) -> str:
    text = (text or "").strip()
    if len(text) <= 1024:
        return text
    return text[:1000].rstrip() + "\n……"


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        data={"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True},
        timeout=30,
    )
    print("sendMessage:", resp.status_code, resp.text[:300])
    return resp


def send_telegram_photo(photo_path: str, caption: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    with open(photo_path, "rb") as f:
        resp = requests.post(
            url,
            data={"chat_id": CHAT_ID, "caption": safe_caption(caption)},
            files={"photo": f},
            timeout=30,
        )
    print("sendPhoto:", resp.status_code, resp.text[:300])
    return resp


def send_with_image(text: str, image: str = ""):
    if image and os.path.isfile(image):
        resp = send_telegram_photo(image, text)
        if resp.status_code == 200:
            return resp
        print("图片发送失败，改为纯文字")
    return send_telegram_message(text)


# =========================
# 调度逻辑
# =========================

def build_candidates() -> List[dict]:
    candidates = []

    for symbol, display in SYMBOLS.items():
        try:
            analysis = analyze_symbol(symbol, display)
            state = get_state(symbol)
            event = detect_event(analysis, state)

            # 首次初始化
            if not state and FIRST_RUN_INIT_ONLY:
                print(f"{symbol} 初始化：{analysis['signal']}")
                upsert_state(symbol, display, analysis["signal"], analysis["support"], analysis["resistance"])
                continue

            posts_today = symbol_posts_today(symbol)
            needs_daily_coverage = posts_today < MIN_SYMBOL_POSTS_PER_DAY
            under_symbol_max = posts_today < MAX_SYMBOL_POSTS_PER_DAY

            # 默认事件触发
            if event["should_send"] and under_symbol_max:
                candidates.append({
                    "analysis": analysis,
                    "event": event,
                    "priority": event.get("priority", 50) + abs(analysis.get("change_1h", 0)),
                })
            # 每币每日保底：没有事件也补一条每日监控
            elif needs_daily_coverage and under_symbol_max:
                coverage_event = {
                    "event_type": "daily_coverage",
                    "event_key": f"{symbol}:daily:{datetime.now().strftime('%Y%m%d')}",
                    "should_send": True,
                    "event_note": "每日主流币监控补位",
                    "priority": 40 + abs(analysis.get("change_1h", 0)),
                }
                candidates.append({
                    "analysis": analysis,
                    "event": coverage_event,
                    "priority": coverage_event["priority"],
                })
            else:
                print(f"{symbol} 无触发：{analysis['signal']}")

            # 不管是否发送，都更新结构状态，但不覆盖 event_key，避免同事件重复刷屏
            upsert_state(symbol, display, analysis["signal"], analysis["support"], analysis["resistance"])

        except Exception as e:
            print(f"{symbol} 分析失败:", e)

        time.sleep(0.8)

    candidates.sort(key=lambda x: x["priority"], reverse=True)
    return candidates


def process_market_posts() -> int:
    if not global_limits_ok():
        return 0

    candidates = build_candidates()
    sent_count = 0

    for candidate in candidates:
        if sent_count >= MAX_POSTS_PER_LOOP:
            break

        if not global_limits_ok():
            break

        analysis = candidate["analysis"]
        event = candidate["event"]
        symbol = analysis["symbol"]

        if not symbol_limits_ok(symbol):
            continue

        # 同事件已发过则跳过，但每日补位不受旧 event_key 影响
        state = get_state(symbol)
        if state and event["event_type"] != "daily_coverage" and event["event_key"] == state.get("last_event_key"):
            print(f"{symbol} 同事件已发过，跳过")
            continue

        news_context = fetch_news_context(symbol, analysis["display"], limit=2) if ENABLE_NEWS_CONTEXT else []
        # 如果关联到新闻，则让内容类型更偏“新闻驱动”
        if news_context and event["event_type"] == "daily_coverage":
            event = event.copy()
            event["event_type"] = "news_driven"
            event["event_note"] = "结合相关新闻做行情背景分析"
            event["event_key"] = f"{symbol}:news:{datetime.now().strftime('%Y%m%d')}:{make_fingerprint(news_context[0]['title'])[:8]}"

        message = build_market_message(analysis, event, news_context)
        image = get_symbol_image_path(symbol)
        resp = send_with_image(message, image)

        now = time.time()
        if resp.status_code == 200:
            print(f"{symbol} 已发送：{event['event_type']} / {analysis['signal']}")
            set_meta("last_global_sent_at", str(now))
            record_sent("market", symbol, event["event_type"], title=message[:80])
            upsert_state(
                symbol,
                analysis["display"],
                analysis["signal"],
                analysis["support"],
                analysis["resistance"],
                event_key=event["event_key"],
                sent_at=now,
            )
            for n in news_context:
                mark_news_used(n["link"])
            sent_count += 1
            time.sleep(2)
        else:
            print(f"{symbol} 发送失败")

    return sent_count


def lm_should_send() -> bool:
    if not ENABLE_LM_COLUMN:
        return False

    last = float(get_meta("last_lm_sent_at", "0") or 0)
    if last <= 0:
        return LM_FIRST_RUN_SEND

    return time.time() - last >= LM_POST_INTERVAL_SECONDS


def process_lm_column() -> bool:
    if not lm_should_send():
        return False

    if total_posts_today() >= MAX_POSTS_PER_DAY or total_posts_last_hour() >= MAX_POSTS_PER_HOUR:
        print("撸毛栏目因全局上限跳过")
        return False

    news_list = fetch_airdrop_news(limit=3) if ENABLE_AIRDROP_RSS else []
    if not news_list:
        print("撸毛/空投栏目没有 RSS 证据，本轮跳过")
        return False

    news = news_list[0]
    message = build_airdrop_message(news)
    image = get_lm_image_path()
    resp = send_with_image(message, image)

    if resp.status_code == 200:
        now = time.time()
        set_meta("last_lm_sent_at", str(now))
        record_sent("lm", "", "airdrop_rss" if news else "lm_generic", title=message[:80])
        if news:
            mark_news_used(news["link"])
        print("撸毛/空投栏目已发送")
        return True

    print("撸毛/空投栏目发送失败")
    return False


# =========================
# 主程序
# =========================

def main():
    if not BOT_TOKEN:
        raise ValueError("缺少环境变量 BOT_TOKEN")
    if not CHAT_ID:
        raise ValueError("缺少环境变量 CHAT_ID")

    init_db()
    refresh_symbols_if_needed(force=True)
    update_news_cache(force=True)
    generate_hotword_queue_if_needed(force=True)
    generate_extra_column_queue_if_needed(force=True)

    print("币圈监控小助手启动成功（10币 + 新闻行情 + 空投RSS + 热词监控 + 扩展栏目 + 控频重构版）")
    print("频道:", CHAT_ID)
    print("当前监控:", ", ".join([f"{v}({k})" for k, v in SYMBOLS.items()]))
    print("图片目录:", IMAGES_DIR)
    print("检查间隔:", CHECK_INTERVAL)
    print("同币冷却:", SYMBOL_COOLDOWN_SECONDS)
    print("全频道冷却:", GLOBAL_COOLDOWN_SECONDS)
    print("每币每日:", f"至少{MIN_SYMBOL_POSTS_PER_DAY}条 / 最多{MAX_SYMBOL_POSTS_PER_DAY}条")
    print("全频道上限:", f"每日{MAX_POSTS_PER_DAY}条 / 每小时{MAX_POSTS_PER_HOUR}条")
    print("AI:", "开启" if USE_AI_POLISH and client else "关闭或未配置")
    print("新闻背景:", "开启" if ENABLE_NEWS_CONTEXT else "关闭")
    print("空投RSS:", "开启" if ENABLE_AIRDROP_RSS else "关闭")
    print("币圈热词监控:", "开启" if ENABLE_HOTWORD_COLUMN else "关闭")
    print("热词配图:", HOTWORD_IMAGE)
    print("热词发送时间:", ", ".join(hotword_send_times()))
    print("扩展栏目:", "开启" if ENABLE_EXTRA_COLUMNS else "关闭")
    print("扩展栏目类型:", ", ".join(enabled_extra_column_types()))
    print("扩展栏目每日:", EXTRA_COLUMN_POSTS_PER_DAY, "条")
    print("扩展栏目发送时间:", ", ".join(extra_column_send_times()))

    if not has_any_state():
        print("首次启动：将初始化币种状态，避免一启动就刷旧信号")

    while True:
        print(f"\n[{datetime.now()}] 开始新一轮检查")

        try:
            refresh_symbols_if_needed(force=False)
        except Exception as e:
            print("刷新市值币种失败:", e)

        try:
            update_news_cache(force=False)
        except Exception as e:
            print("更新新闻失败:", e)

        try:
            process_hotword_column()
        except Exception as e:
            print("币圈热词监控处理失败:", e)

        try:
            process_extra_columns()
        except Exception as e:
            print("扩展栏目处理失败:", e)

        try:
            sent_market = process_market_posts()
            print("本轮行情发送:", sent_market)
        except Exception as e:
            print("行情处理失败:", e)

        try:
            process_lm_column()
        except Exception as e:
            print("撸毛栏目处理失败:", e)

        print(f"休眠 {CHECK_INTERVAL} 秒")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
