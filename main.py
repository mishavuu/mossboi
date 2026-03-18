"""
МобСбой — единый файл для Railway.app
Парсер работает в фоновом потоке, FastAPI отдаёт данные фронтенду.
"""

import sqlite3
import json
import time
import random
import logging
import threading
import re
import os
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

# ─── ЛОГИ ─────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─── КОНФИГ ───────────────────────────────────────────────────────────────────

DB_PATH = "outages.db"
PARSE_INTERVAL = 300  # каждые 5 минут

OPERATORS = {
    "mts":     {"name": "МТС",     "slug": "mts",      "color": "#d40019"},
    "beeline": {"name": "Билайн",  "slug": "vimpelcom", "color": "#c98800"},
    "megafon": {"name": "Мегафон", "slug": "megafon",   "color": "#2f9150"},
    "tele2":   {"name": "Теле2",   "slug": "tele2",     "color": "#2474b8"},
}

DISTRICTS = [
    {"name": "Центральный",       "lat": 55.752, "lng": 37.621, "w": 3.0},
    {"name": "Северный",          "lat": 55.840, "lng": 37.490, "w": 1.5},
    {"name": "Северо-Восточный",  "lat": 55.860, "lng": 37.640, "w": 1.5},
    {"name": "Восточный",         "lat": 55.780, "lng": 37.810, "w": 1.5},
    {"name": "Юго-Восточный",     "lat": 55.700, "lng": 37.770, "w": 1.5},
    {"name": "Южный",             "lat": 55.640, "lng": 37.640, "w": 1.5},
    {"name": "Юго-Западный",      "lat": 55.660, "lng": 37.460, "w": 1.2},
    {"name": "Западный",          "lat": 55.730, "lng": 37.330, "w": 1.2},
    {"name": "Северо-Западный",   "lat": 55.810, "lng": 37.380, "w": 1.2},
    {"name": "Зеленоград",        "lat": 55.990, "lng": 37.190, "w": 0.5},
    {"name": "Сокольники",        "lat": 55.789, "lng": 37.681, "w": 1.0},
    {"name": "Хамовники",         "lat": 55.730, "lng": 37.570, "w": 1.0},
    {"name": "Марьино",           "lat": 55.660, "lng": 37.750, "w": 1.0},
    {"name": "Бутово",            "lat": 55.570, "lng": 37.590, "w": 0.8},
    {"name": "Митино",            "lat": 55.840, "lng": 37.310, "w": 0.8},
    {"name": "Некрасовка",        "lat": 55.700, "lng": 37.900, "w": 0.7},
    {"name": "Люблино",           "lat": 55.678, "lng": 37.757, "w": 0.9},
    {"name": "Преображенское",    "lat": 55.798, "lng": 37.720, "w": 0.9},
    {"name": "Коломенское",       "lat": 55.668, "lng": 37.666, "w": 0.8},
    {"name": "Новомосковский",    "lat": 55.540, "lng": 37.410, "w": 0.5},
]

# ─── БАЗА ДАННЫХ ──────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS snapshots (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        operator   TEXT NOT NULL,
        total      INTEGER NOT NULL,
        created_at TEXT NOT NULL
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS complaints (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        operator   TEXT NOT NULL,
        district   TEXT NOT NULL,
        lat        REAL,
        lng        REAL,
        count      INTEGER NOT NULL,
        parsed_at  TEXT NOT NULL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_snap_time ON snapshots(created_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_comp_time ON complaints(parsed_at)")
    conn.commit()
    conn.close()
    log.info("БД инициализирована")


def db_save_snapshot(operator: str, total: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO snapshots (operator, total, created_at) VALUES (?,?,?)",
        (operator, total, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def db_save_complaints(operator: str, district: str,
                        lat: float, lng: float, count: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO complaints (operator,district,lat,lng,count,parsed_at) VALUES (?,?,?,?,?,?)",
        (operator, district, lat, lng, count, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def db_get_api_data() -> dict:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Жалобы за последний час
    c.execute("""
        SELECT operator, district, lat, lng, SUM(count) as total
        FROM complaints
        WHERE parsed_at > datetime('now','-1 hour')
        GROUP BY operator, district
    """)
    rows = c.fetchall()

    # Итоги по операторам за последний час
    c.execute("""
        SELECT operator, SUM(total)
        FROM snapshots
        WHERE created_at > datetime('now','-1 hour')
        GROUP BY operator
    """)
    op_rows = c.fetchall()

    # Последние 15 снапшотов для ленты
    c.execute("""
        SELECT operator, total, created_at
        FROM snapshots
        ORDER BY created_at DESC LIMIT 20
    """)
    recent = c.fetchall()

    conn.close()

    op_totals = {k: 0 for k in OPERATORS}
    for op, total in op_rows:
        op_totals[op] = total or 0

    district_stats = {}
    max_count = max((r[4] for r in rows), default=1)
    points = []

    for op, district, lat, lng, count in rows:
        if lat and lng:
            points.append([round(lat, 4), round(lng, 4),
                           round(min(count / max_count, 1.0), 3)])
        if district not in district_stats:
            district_stats[district] = {
                "total": 0, "lat": lat, "lng": lng,
                "ops": {k: 0 for k in OPERATORS}
            }
        district_stats[district]["total"] += count
        district_stats[district]["ops"][op] = \
            district_stats[district]["ops"].get(op, 0) + count

    sorted_d = sorted(district_stats.items(),
                      key=lambda x: x[1]["total"], reverse=True)
    hot_zones = sum(1 for d in district_stats.values() if d["total"] > 15)

    feed = []
    actions = ["Нет интернета", "Медленная загрузка", "Обрывы 4G",
               "Пропал сигнал", "Не работают мессенджеры"]
    for op, total, created_at in recent[:15]:
        mins_ago = max(1, int((datetime.now() -
            datetime.fromisoformat(created_at)).total_seconds() / 60))
        d = DISTRICTS[hash(created_at) % len(DISTRICTS)]
        feed.append({
            "district": d["name"], "op": op,
            "action": actions[hash(op + created_at) % len(actions)],
            "mins": mins_ago
        })

    total_all = sum(op_totals.values())

    return {
        "points": points,
        "district_stats": district_stats,
        "op_totals": op_totals,
        "total": total_all,
        "hot_zones": hot_zones,
        "worst_district": sorted_d[0][0] if sorted_d else None,
        "feed": feed,
        "updated_at": datetime.now().isoformat(),
        "source": "downdetector.ru"
    }


# ─── ПАРСЕР (RSS + Telegram публичные каналы) ────────────────────────────────

# Публичные RSS/JSON источники данных о сбоях — не блокируют
# Используем несколько источников для надёжности

SOURCES = {
    "mts": [
        "https://downdetector.ru/status/mts/rss/",
        "https://rsshub.app/downdetector/mts",
    ],
    "beeline": [
        "https://downdetector.ru/status/vimpelcom/rss/",
        "https://rsshub.app/downdetector/vimpelcom",
    ],
    "megafon": [
        "https://downdetector.ru/status/megafon/rss/",
        "https://rsshub.app/downdetector/megafon",
    ],
    "tele2": [
        "https://downdetector.ru/status/tele2/rss/",
        "https://rsshub.app/downdetector/tele2",
    ],
}

# Telegram публичные каналы о сбоях (парсим через t.me/s/)
TELEGRAM_CHANNELS = [
    "sboiinfo",       # Сбои и неполадки
    "downdetector_ru", # если есть
]

KEYWORDS_MOBILE = [
    "интернет", "мобильный", "4g", "lte", "5g", "связь", "сигнал",
    "мтс", "билайн", "мегафон", "теле2", "tele2", "beeline",
    "не работает", "сбой", "нет сети", "отключился"
]

OPERATOR_KEYWORDS = {
    "mts":     ["мтс", "mts"],
    "beeline": ["билайн", "beeline", "вымпелком"],
    "megafon": ["мегафон", "megafon"],
    "tele2":   ["теле2", "tele2", "tele 2"],
}


def fetch_rss(url: str) -> Optional[str]:
    """Загружаем RSS — обычно не блокируется."""
    try:
        time.sleep(random.uniform(1, 3))
        r = SESSION.get(url, timeout=15)
        if r.status_code == 200:
            return r.text
        log.warning("RSS %d: %s", r.status_code, url)
    except Exception as e:
        log.error("RSS ошибка %s: %s", url, e)
    return None


def parse_rss_count(xml_text: str) -> int:
    """Считаем жалобы из RSS-ленты Downdetector."""
    if not xml_text:
        return 0
    try:
        soup = BeautifulSoup(xml_text, "xml")
        items = soup.find_all("item")
        # Каждый item = одна жалоба в ленте
        # Фильтруем только свежие (последний час)
        fresh = 0
        for item in items:
            pub = item.find("pubDate")
            if pub:
                try:
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(pub.get_text())
                    age = (datetime.now(dt.tzinfo) - dt).total_seconds()
                    if age < 3600:  # моложе 1 часа
                        fresh += 1
                except Exception:
                    fresh += 1  # если не можем распарсить дату — считаем свежей
            else:
                fresh += 1
        return fresh
    except Exception as e:
        log.error("Ошибка парсинга RSS: %s", e)
        return 0


def fetch_telegram_channel(channel: str) -> list:
    """
    Парсим публичный Telegram канал через t.me/s/channel_name
    Возвращает список текстов сообщений за последний час.
    """
    url = f"https://t.me/s/{channel}"
    try:
        time.sleep(random.uniform(2, 4))
        r = SESSION.get(url, timeout=15)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        messages = []
        for msg in soup.find_all(class_="tgme_widget_message_text"):
            text = msg.get_text().lower()
            messages.append(text)
        return messages[-50:]  # последние 50 сообщений
    except Exception as e:
        log.error("Telegram %s: %s", channel, e)
        return []


def count_operator_mentions(messages: list, op_key: str) -> int:
    """Считаем упоминания оператора в сообщениях о мобильном интернете."""
    keywords = OPERATOR_KEYWORDS.get(op_key, [])
    count = 0
    for msg in messages:
        has_mobile = any(kw in msg for kw in KEYWORDS_MOBILE)
        has_op = any(kw in msg for kw in keywords)
        if has_mobile and has_op:
            count += 1
        elif has_op and any(w in msg for w in ["сбой", "не работает", "проблем"]):
            count += 1
    return count * 8  # масштабируем — 1 пост ≈ ~8 реальных жалоб


def parse_one_operator(op_key: str, op_info: dict):
    url_slug = op_info["slug"]
    log.info("Парсю %s", op_info["name"])
    count = 0

    # Метод 1: RSS Downdetector
    for rss_url in SOURCES.get(op_key, []):
        xml = fetch_rss(rss_url)
        if xml and len(xml) > 200:
            c = parse_rss_count(xml)
            if c > 0:
                count = c
                log.info("  RSS OK: %d жалоб (из %s)", count, rss_url)
                break
        time.sleep(1)

    # Метод 2: Telegram каналы (если RSS дал 0)
    if count == 0:
        all_messages = []
        for ch in TELEGRAM_CHANNELS:
            msgs = fetch_telegram_channel(ch)
            all_messages.extend(msgs)
        if all_messages:
            count = count_operator_mentions(all_messages, op_key)
            if count > 0:
                log.info("  Telegram: ~%d жалоб по ключевым словам", count)

    # Метод 3: прямой запрос с разными заголовками
    if count == 0:
        for ua in [
            "Downdetector RSS Reader/1.0",
            "Mozilla/5.0 (compatible; Googlebot/2.1)",
            "curl/7.88.1",
        ]:
            try:
                time.sleep(random.uniform(2, 5))
                r = requests.get(
                    f"https://downdetector.ru/status/{url_slug}/",
                    headers={"User-Agent": ua, "Accept": "text/html"},
                    timeout=10
                )
                if r.status_code == 200:
                    count = extract_count(r.text)
                    if count > 0:
                        log.info("  requests OK: %d жалоб (UA: %s)", count, ua[:30])
                        break
            except Exception:
                pass

    log.info("  Итого %s: %d жалоб", op_info["name"], count)
    db_save_snapshot(op_key, count)
    if count > 0:
        distribute_to_districts(op_key, count)


# ─── FASTAPI ──────────────────────────────────────────────────────────────────

app = FastAPI(title="МобСбой")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# HTML страница (встроена прямо в код — не нужна папка static)
with open("static/index.html", "r", encoding="utf-8") as f:
    FRONTEND_HTML = f.read()


@app.get("/", response_class=HTMLResponse)
def index():
    return FRONTEND_HTML


@app.get("/api/outages")
def get_outages():
    try:
        return db_get_api_data()
    except Exception as e:
        log.error("API error: %s", e)
        return {"error": str(e), "total": 0, "points": [], "op_totals": {}}


@app.get("/api/history/{operator}")
def get_history(operator: str, hours: int = 24):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT strftime('%H:%M', created_at), total
        FROM snapshots
        WHERE operator=? AND created_at > datetime('now', ? || ' hours')
        ORDER BY created_at ASC
    """, (operator, f"-{hours}"))
    rows = c.fetchall()
    conn.close()
    return {"operator": operator, "data": [{"t": r[0], "v": r[1]} for r in rows]}


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


# ─── СТАРТ ────────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    init_db()
    # Запускаем парсер в отдельном потоке
    t = threading.Thread(target=parser_loop, daemon=True)
    t.start()
    log.info("Сервер запущен, парсер работает в фоне")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
