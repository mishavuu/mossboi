"""
МобСбой — парсер Downdetector.ru
Запускать: python parser.py
Требования: pip install requests beautifulsoup4 playwright sqlite3
"""

import sqlite3
import json
import time
import random
import logging
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ─── НАСТРОЙКИ ────────────────────────────────────────────────────────────────

DB_PATH = "outages.db"
PARSE_INTERVAL = 300  # секунд между запросами (5 минут)

# Операторы и их slug-имена на Downdetector
OPERATORS = {
    "mts":     { "name": "МТС",     "slug": "mts",     "color": "#d40019" },
    "beeline": { "name": "Билайн",  "slug": "vimpelcom","color": "#c98800" },
    "megafon": { "name": "Мегафон", "slug": "megafon",  "color": "#2f9150" },
    "tele2":   { "name": "Теле2",   "slug": "tele2",    "color": "#2474b8" },
}

# Районы Москвы с координатами для геокодинга жалоб
DISTRICTS = [
    {"name": "Центральный",       "lat": 55.752, "lng": 37.621, "keywords": ["центр", "кремль", "арбат", "тверская", "замоскворечье"]},
    {"name": "Северный",          "lat": 55.840, "lng": 37.490, "keywords": ["север", "войковская", "речной", "дмитровская"]},
    {"name": "Северо-Восточный",  "lat": 55.860, "lng": 37.640, "keywords": ["алтуфьево", "медведково", "бибирево", "отрадное"]},
    {"name": "Восточный",         "lat": 55.780, "lng": 37.810, "keywords": ["измайлово", "новогиреево", "перово", "балашиха"]},
    {"name": "Юго-Восточный",     "lat": 55.700, "lng": 37.770, "keywords": ["люблино", "марьино", "печатники", "капотня"]},
    {"name": "Южный",             "lat": 55.640, "lng": 37.640, "keywords": ["орехово", "царицыно", "бирюлево", "нагатино"]},
    {"name": "Юго-Западный",      "lat": 55.660, "lng": 37.460, "keywords": ["ясенево", "теплый стан", "коньково", "беляево"]},
    {"name": "Западный",          "lat": 55.730, "lng": 37.330, "keywords": ["кунцево", "фили", "крылатское", "можайский"]},
    {"name": "Северо-Западный",   "lat": 55.810, "lng": 37.380, "keywords": ["строгино", "митино", "тушино", "щукино"]},
    {"name": "Зеленоград",        "lat": 55.990, "lng": 37.190, "keywords": ["зеленоград"]},
    {"name": "Сокольники",        "lat": 55.789, "lng": 37.681, "keywords": ["сокольники", "преображенское"]},
    {"name": "Хамовники",         "lat": 55.730, "lng": 37.570, "keywords": ["хамовники", "парк культуры"]},
    {"name": "Марьино",           "lat": 55.660, "lng": 37.750, "keywords": ["марьино", "братиславская"]},
    {"name": "Бутово",            "lat": 55.570, "lng": 37.590, "keywords": ["бутово", "южное бутово"]},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("parser.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── БАЗА ДАННЫХ ──────────────────────────────────────────────────────────────

def init_db():
    """Создаём таблицы если не существуют."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS complaints (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            operator    TEXT NOT NULL,
            count       INTEGER NOT NULL,
            district    TEXT,
            lat         REAL,
            lng         REAL,
            parsed_at   TEXT NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            operator    TEXT NOT NULL,
            total       INTEGER NOT NULL,
            chart_json  TEXT,
            created_at  TEXT NOT NULL
        )
    """)

    # Индексы для быстрых запросов
    c.execute("CREATE INDEX IF NOT EXISTS idx_complaints_op ON complaints(operator)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_complaints_time ON complaints(parsed_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_op ON snapshots(operator)")

    conn.commit()
    conn.close()
    log.info("База данных инициализирована: %s", DB_PATH)


def save_snapshot(operator: str, total: int, chart_data: list):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO snapshots (operator, total, chart_json, created_at) VALUES (?, ?, ?, ?)",
        (operator, total, json.dumps(chart_data), datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def save_complaint(operator: str, count: int, district: Optional[str],
                   lat: Optional[float], lng: Optional[float]):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO complaints (operator, count, district, lat, lng, parsed_at) VALUES (?,?,?,?,?,?)",
        (operator, count, district, lat, lng, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


# ─── HTTP ────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://downdetector.ru/",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def fetch_page(url: str, retries: int = 3) -> Optional[str]:
    """Загружаем страницу с повторными попытками."""
    for attempt in range(retries):
        try:
            # Случайная задержка чтобы не триггерить anti-bot
            time.sleep(random.uniform(2.5, 5.5))
            r = SESSION.get(url, timeout=15)
            if r.status_code == 200:
                return r.text
            elif r.status_code == 403:
                log.warning("403 на %s — нужен прокси или Playwright", url)
                return None
            else:
                log.warning("HTTP %s на %s", r.status_code, url)
        except requests.RequestException as e:
            log.error("Ошибка запроса (попытка %d): %s", attempt + 1, e)
            time.sleep(5 * (attempt + 1))
    return None


# ─── ПАРСЕР ──────────────────────────────────────────────────────────────────

def parse_complaint_count(html: str) -> int:
    """
    Извлекаем текущее число жалоб с главной страницы оператора.
    Downdetector показывает число в теге с классом 'num-reports'.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Метод 1: прямой тег с числом жалоб
    tag = soup.find(class_="num-reports")
    if tag:
        try:
            return int(tag.get_text(strip=True).replace(" ", "").replace("\xa0", ""))
        except ValueError:
            pass

    # Метод 2: JSON в script-тегах (Downdetector часто прячет данные в JS)
    for script in soup.find_all("script"):
        text = script.string or ""
        if "reports_by_time" in text or "complaincount" in text:
            # Ищем паттерн числа жалоб
            import re
            m = re.search(r'"count"\s*:\s*(\d+)', text)
            if m:
                return int(m.group(1))

    # Метод 3: парсим chart data из inline JSON
    for script in soup.find_all("script", type="application/json"):
        try:
            data = json.loads(script.string or "{}")
            if "series" in data:
                series = data["series"]
                if series and isinstance(series, list):
                    # Берём последнее значение из временного ряда
                    last = series[-1] if series else {}
                    return int(last.get("y", 0))
        except (json.JSONDecodeError, KeyError, TypeError):
            pass

    log.warning("Не удалось распарсить число жалоб")
    return 0


def parse_chart_data(html: str) -> list:
    """Извлекаем исторический график жалоб за последние 24 часа."""
    soup = BeautifulSoup(html, "html.parser")
    import re

    for script in soup.find_all("script"):
        text = script.string or ""
        if "reports_by_time" in text:
            m = re.search(r'reports_by_time\s*=\s*(\[.*?\]);', text, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(1))
                except json.JSONDecodeError:
                    pass
    return []


def scrape_operator(op_key: str, op_info: dict) -> dict:
    """Парсим одного оператора."""
    url = f"https://downdetector.ru/status/{op_info['slug']}/"
    log.info("Парсю %s → %s", op_info["name"], url)

    html = fetch_page(url)
    if not html:
        return {"operator": op_key, "count": 0, "error": "fetch_failed"}

    count = parse_complaint_count(html)
    chart = parse_chart_data(html)

    log.info("  %s: %d жалоб", op_info["name"], count)

    save_snapshot(op_key, count, chart)

    # Распределяем жалобы по районам (простая эвристика)
    distribute_by_districts(op_key, count)

    return {"operator": op_key, "name": op_info["name"], "count": count, "chart": chart}


def distribute_by_districts(operator: str, total: int):
    """
    Раскидываем жалобы по районам.
    В реальной версии: геокодируем адреса из текстов жалоб.
    Сейчас: случайное взвешенное распределение.
    """
    if total == 0:
        return

    # Центр и крупные районы получают больше жалоб
    weights = [3, 1.5, 1.5, 1.5, 1.5, 1.5, 1, 1, 1, 0.5, 1, 1, 1, 0.8]
    total_w = sum(weights)

    for i, district in enumerate(DISTRICTS):
        share = int(total * (weights[i] / total_w) * random.uniform(0.6, 1.4))
        if share > 0:
            save_complaint(
                operator=operator,
                count=share,
                district=district["name"],
                lat=district["lat"] + random.uniform(-0.03, 0.03),
                lng=district["lng"] + random.uniform(-0.04, 0.04),
            )


# ─── API ENDPOINT DATA ────────────────────────────────────────────────────────

def build_api_response() -> dict:
    """
    Собираем данные для отдачи фронтенду через /api/outages.
    Вызывается из FastAPI/Flask.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Жалобы за последний час
    c.execute("""
        SELECT operator, district, lat, lng, SUM(count) as total
        FROM complaints
        WHERE parsed_at > datetime('now', '-1 hour')
        GROUP BY operator, district
        ORDER BY total DESC
    """)
    rows = c.fetchall()

    # Точки для тепловой карты
    points = []
    district_stats = {}
    op_totals = {k: 0 for k in OPERATORS}

    max_count = max((r[4] for r in rows), default=1)

    for op, district, lat, lng, count in rows:
        if lat and lng:
            intensity = min(count / max_count, 1.0)
            points.append([lat, lng, intensity])

        op_totals[op] = op_totals.get(op, 0) + count

        if district not in district_stats:
            district_stats[district] = {"total": 0, "ops": {k: 0 for k in OPERATORS}, "lat": lat, "lng": lng}
        district_stats[district]["total"] += count
        district_stats[district]["ops"][op] = district_stats[district]["ops"].get(op, 0) + count

    # Топ районов
    sorted_districts = sorted(district_stats.items(), key=lambda x: x[1]["total"], reverse=True)

    # Последние жалобы для ленты (из снапшотов)
    c.execute("""
        SELECT operator, total, created_at
        FROM snapshots
        ORDER BY created_at DESC
        LIMIT 20
    """)
    recent = c.fetchall()
    conn.close()

    total_complaints = sum(op_totals.values())
    hot_zones = sum(1 for d in district_stats.values() if d["total"] > 20)

    return {
        "points": points,
        "district_stats": district_stats,
        "op_totals": op_totals,
        "total": total_complaints,
        "hot_zones": hot_zones,
        "worst_district": sorted_districts[0][0] if sorted_districts else None,
        "updated_at": datetime.now().isoformat(),
    }


# ─── ОСНОВНОЙ ЦИКЛ ────────────────────────────────────────────────────────────

def run_parser():
    """Бесконечный цикл парсинга."""
    log.info("=== МобСбой парсер запущен ===")
    init_db()

    cycle = 0
    while True:
        cycle += 1
        log.info("--- Цикл #%d ---", cycle)

        results = []
        for op_key, op_info in OPERATORS.items():
            try:
                result = scrape_operator(op_key, op_info)
                results.append(result)
            except Exception as e:
                log.error("Ошибка при парсинге %s: %s", op_key, e)

            # Пауза между операторами
            time.sleep(random.uniform(3, 7))

        total = sum(r.get("count", 0) for r in results)
        log.info("Итого жалоб: %d. Следующий цикл через %d сек.", total, PARSE_INTERVAL)
        time.sleep(PARSE_INTERVAL)


if __name__ == "__main__":
    run_parser()
