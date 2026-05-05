"""
Пакетное сохранение записей в БД.
Использует executemany для небольших пакетов и COPY для массовых загрузок.
"""
import json
import logging
from typing import Any

from psycopg import AsyncConnection

from database import get_connection

logger = logging.getLogger(__name__)

# Порог, после которого переключаемся с executemany на COPY
COPY_THRESHOLD = 1000


async def batch_save_production(records: list[dict[str, Any]]) -> int:
    """
    Пакетное сохранение валидных записей в hr.employees.
    До COPY_THRESHOLD — executemany, выше — COPY.
    """
    if not records:
        return 0

    rows = []
    for r in records:
        passport_data = None
        series = r.get("passport_series")
        number = r.get("passport_number")
        if series and number:
            passport_data = f"{series} {number}"

        rows.append((
            r["last_name"],
            r["first_name"],
            r.get("middle_name"),
            r["birth_date"],
            r["hire_date"],
            r.get("termination_date"),
            r["position"],
            r["salary"],
            passport_data,
        ))

    async with get_connection() as conn:
        if len(rows) >= COPY_THRESHOLD:
            saved = await _copy_production(conn, rows)
        else:
            saved = await _executemany_production(conn, rows)

    logger.info(f"Saved to production: {saved} rows (mode={'COPY' if saved >= COPY_THRESHOLD else 'executemany'})")
    return saved


async def _executemany_production(conn: AsyncConnection, rows: list[tuple]) -> int:
    sql = """
        INSERT INTO hr.employees (
            last_name, first_name, middle_name,
            birth_date, hire_date, termination_date,
            position, salary, passport_data, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """
    async with conn.cursor() as cur:
        await cur.executemany(sql, rows)
    return len(rows)


async def _copy_production(conn: AsyncConnection, rows: list[tuple]) -> int:
    """COPY — самый быстрый способ массовой вставки."""
    copy_sql = """
        COPY hr.employees (
            last_name, first_name, middle_name,
            birth_date, hire_date, termination_date,
            position, salary, passport_data
        ) FROM STDIN
    """
    async with conn.cursor() as cur:
        async with cur.copy(copy_sql) as copy:
            for row in rows:
                await copy.write_row(row)
    return len(rows)


async def batch_save_quarantine(records: list[tuple[dict, list]]) -> int:
    """Пакетное сохранение отбракованных записей в data_quality.quarantine_log."""
    if not records:
        return 0

    rows = []
    for raw_row, errors in records:
        clean: dict[str, Any] = {}
        for k, v in raw_row.items():
            if v is None:
                clean[k] = None
            elif hasattr(v, "isoformat"):
                clean[k] = v.isoformat()
            elif isinstance(v, (int, float, str, bool)):
                clean[k] = v
            else:
                clean[k] = str(v)

        rows.append((
            json.dumps(clean, ensure_ascii=False),
            json.dumps(errors, ensure_ascii=False),
        ))

    sql = """
        INSERT INTO data_quality.quarantine_log (raw_data, validation_errors, created_at)
        VALUES (%s::jsonb, %s::jsonb, NOW())
    """
    async with get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(sql, rows)

    logger.info(f"Saved to quarantine: {len(rows)} rows")
    return len(rows)
