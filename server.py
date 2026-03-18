"""
МобСбой — бэкенд сервер (FastAPI)
Запуск: uvicorn server:app --host 0.0.0.0 --port 8000
"""

import sqlite3
import json
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# Импортируем функцию сборки ответа из парсера
from parser import build_api_response, init_db, DB_PATH

app = FastAPI(title="МобСбой API", version="1.0.0")

# Разрешаем запросы с любого домена (нужно для фронтенда)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Отдаём статику (index.html, карта)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/")
def index():
    """Главная страница — отдаём index.html."""
    return FileResponse("static/index.html")


@app.get("/api/outages")
def get_outages():
    """
    Основной эндпоинт для фронтенда.
    Возвращает жалобы за последний час, точки для карты, статистику.
    """
    try:
        data = build_api_response()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history/{operator}")
def get_history(operator: str, hours: int = 24):
    """
    История жалоб по конкретному оператору.
    Используется для графиков динамики.
    """
    if operator not in ["mts", "beeline", "megafon", "tele2"]:
        raise HTTPException(status_code=400, detail="Неизвестный оператор")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT strftime('%H:%M', created_at) as time, total
        FROM snapshots
        WHERE operator = ?
          AND created_at > datetime('now', ? || ' hours')
        ORDER BY created_at ASC
    """, (operator, f"-{hours}"))
    rows = c.fetchall()
    conn.close()

    return {"operator": operator, "data": [{"time": r[0], "count": r[1]} for r in rows]}


@app.get("/api/stats")
def get_stats():
    """Общая статистика за последние 24 часа."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        SELECT operator, SUM(count)
        FROM complaints
        WHERE parsed_at > datetime('now', '-24 hours')
        GROUP BY operator
    """)
    rows = c.fetchall()
    conn.close()

    return {"period": "24h", "by_operator": {r[0]: r[1] for r in rows}}


@app.get("/health")
def health():
    """Проверка работоспособности — для мониторинга."""
    return {"status": "ok", "time": datetime.now().isoformat()}
