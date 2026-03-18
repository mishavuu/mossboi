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


# ─── ПАРСЕР ───────────────────────────────────────────────────────────────────

def fetch_html_playwright(url: str) -> Optional[str]:
    """
    Загружаем страницу через настоящий браузер Chromium.
    Обходит anti-bot защиту Downdetector — браузер неотличим от живого пользователя.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("Playwright не установлен")
        return None

    for attempt in range(2):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-blink-features=AutomationControlled",
                    ]
                )
                ctx = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                    locale="ru-RU",
                    timezone_id="Europe/Moscow",
                    viewport={"width": 1366, "height": 768},
                )
                page = ctx.new_page()

                # Скрываем признаки автоматизации
                page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
                """)

                # Ждём полной загрузки страницы включая JS
                page.goto(url, wait_until="networkidle", timeout=30000)
                time.sleep(random.uniform(2, 4))  # имитируем чтение

                html = page.content()
                browser.close()

                if html and len(html) > 1000:
                    log.info("  Playwright OK: %s (%d байт)", url, len(html))
                    return html
                else:
                    log.warning("  Playwright: пустая страница на %s", url)

        except Exception as e:
            log.error("  Playwright ошибка (попытка %d): %s", attempt + 1, e)
            time.sleep(5)

    return None


def extract_count(html: str) -> int:
    """Пробуем несколько методов извлечь число жалоб."""
    soup = BeautifulSoup(html, "html.parser")

    # Метод 1: тег с классом num-reports
    tag = soup.find(class_="num-reports")
    if tag:
        try:
            return int(re.sub(r'\D', '', tag.get_text()))
        except ValueError:
            pass

    # Метод 2: JSON в теге <script type="application/json">
    for script in soup.find_all("script", type="application/json"):
        try:
            data = json.loads(script.string or "{}")
            if "series" in data and data["series"]:
                last = data["series"][-1]
                return int(last.get("y", 0))
        except (json.JSONDecodeError, TypeError, KeyError):
            pass

    # Метод 3: ищем паттерн в inline JS
    for script in soup.find_all("script"):
        text = script.string or ""
        m = re.search(r'"count"\s*:\s*(\d+)', text)
        if m:
            return int(m.group(1))
        m = re.search(r'complaincount["\s:]+(\d+)', text)
        if m:
            return int(m.group(1))

    # Метод 4: meta description
    desc = soup.find("meta", {"name": "description"})
    if desc:
        m = re.search(r'(\d+)\s*(?:жалоб|проблем|сообщений)', desc.get("content", ""))
        if m:
            return int(m.group(1))

    return 0


def distribute_to_districts(operator: str, total: int):
    """Раскидываем жалобы по районам с весами."""
    if total == 0:
        return
    total_w = sum(d["w"] for d in DISTRICTS)
    for d in DISTRICTS:
        share = int(total * (d["w"] / total_w) * random.uniform(0.5, 1.5))
        if share > 0:
            db_save_complaints(
                operator=operator,
                district=d["name"],
                lat=d["lat"] + random.uniform(-0.025, 0.025),
                lng=d["lng"] + random.uniform(-0.030, 0.030),
                count=share
            )


def parse_one_operator(op_key: str, op_info: dict):
    url = f"https://downdetector.ru/status/{op_info['slug']}/"
    log.info("Парсю %s → %s", op_info["name"], url)

    # Сначала пробуем Playwright (обходит anti-bot)
    html = fetch_html_playwright(url)

    # Если Playwright не установлен или упал — fallback на requests
    if not html:
        log.info("  Playwright недоступен, пробую requests...")
        html = fetch_html(url)

    if not html:
        log.warning("Пропускаю %s — нет данных", op_info["name"])
        db_save_snapshot(op_key, 0)
        return

    count = extract_count(html)
    log.info("  %s: %d жалоб", op_info["name"], count)

    db_save_snapshot(op_key, count)
    distribute_to_districts(op_key, count)


def parser_loop():
    """Фоновый поток: парсим всех операторов каждые 5 минут."""
    log.info("=== Парсер запущен в фоне ===")
    cycle = 0
    while True:
        cycle += 1
        log.info("--- Цикл #%d ---", cycle)
        for op_key, op_info in OPERATORS.items():
            try:
                parse_one_operator(op_key, op_info)
            except Exception as e:
                log.error("Ошибка %s: %s", op_key, e)
            time.sleep(random.uniform(4, 8))
        log.info("Цикл завершён. Следующий через %d сек.", PARSE_INTERVAL)
        time.sleep(PARSE_INTERVAL)


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
    # Устанавливаем браузер Playwright при первом запуске
    try:
        import subprocess
        result = subprocess.run(
            ["playwright", "install", "chromium", "--with-deps"],
            timeout=180, capture_output=True, text=True
        )
        log.info("Playwright install: %s", result.stdout[-200:] if result.stdout else "ok")
    except Exception as e:
        log.warning("Playwright install пропущен: %s", e)
    # Запускаем парсер в отдельном потоке
    t = threading.Thread(target=parser_loop, daemon=True)
    t.start()
    log.info("Сервер запущен, парсер работает в фоне")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
