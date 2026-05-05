"""
Периодическое обновление метрик качества данных.
Все запросы работают асинхронно через пул соединений.
"""
import logging

from database import get_connection
from metrics import (
    update_table_size_metric,
    update_completeness_metric,
    update_overall_score,
    update_freshness_metric,
    update_uniqueness_metric,
    update_duplicate_metric,
    update_anomaly_count,
    update_salary_distribution,
    update_quarantine_size,
)

logger = logging.getLogger(__name__)

TABLE_NAME = "hr.employees"

# Веса колонок для интегральной оценки качества
COMPLETENESS_WEIGHTS = {
    "full_name": 0.25,
    "position": 0.15,
    "salary": 0.15,
    "birth_date": 0.20,
    "hire_date": 0.15,
    "passport_data": 0.10,
}


async def update_all_quality_metrics() -> None:
    """Пересчёт всех метрик качества по hr.employees."""
    try:
        async with get_connection() as conn:
            async with conn.cursor() as cur:
                await _update_table_size(cur)
                await _update_completeness_and_score(cur)
                await _update_freshness(cur)
                await _update_passport_uniqueness(cur)
                await _update_duplicates(cur)
                await _update_salary_anomalies(cur)
                await _update_salary_distribution(cur)
        logger.info("All quality metrics updated successfully")
    except Exception as e:
        logger.error(f"Error updating quality metrics: {e}", exc_info=True)


async def _update_table_size(cur) -> None:
    await cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    row = await cur.fetchone()
    update_table_size_metric(TABLE_NAME, row[0])


async def _update_completeness_and_score(cur) -> None:
    await cur.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN last_name IS NOT NULL AND first_name IS NOT NULL THEN 1 ELSE 0 END) AS full_name_filled,
            SUM(CASE WHEN position IS NOT NULL THEN 1 ELSE 0 END) AS position_filled,
            SUM(CASE WHEN salary IS NOT NULL THEN 1 ELSE 0 END) AS salary_filled,
            SUM(CASE WHEN birth_date IS NOT NULL THEN 1 ELSE 0 END) AS birth_date_filled,
            SUM(CASE WHEN hire_date IS NOT NULL THEN 1 ELSE 0 END) AS hire_date_filled,
            SUM(CASE WHEN passport_data IS NOT NULL THEN 1 ELSE 0 END) AS passport_filled
        FROM {TABLE_NAME}
    """)
    result = await cur.fetchone()
    if not result or result[0] == 0:
        return

    total = result[0]
    completeness_map = {
        "full_name": result[1],
        "position": result[2],
        "salary": result[3],
        "birth_date": result[4],
        "hire_date": result[5],
        "passport_data": result[6],
    }

    for column, filled in completeness_map.items():
        pct = (filled / total) * 100
        update_completeness_metric(TABLE_NAME, column, pct)

    overall_score = sum(
        COMPLETENESS_WEIGHTS.get(col, 0) * (filled / total * 100)
        for col, filled in completeness_map.items()
    )
    update_overall_score(TABLE_NAME, overall_score)


async def _update_freshness(cur) -> None:
    await cur.execute(f"""
        SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at))) / 3600 AS hours
        FROM {TABLE_NAME}
    """)
    row = await cur.fetchone()
    if row and row[0] is not None:
        update_freshness_metric(TABLE_NAME, float(row[0]))


async def _update_passport_uniqueness(cur) -> None:
    await cur.execute(f"""
        SELECT
            COUNT(DISTINCT passport_data)::float / NULLIF(COUNT(*), 0)::float * 100
        FROM {TABLE_NAME}
        WHERE passport_data IS NOT NULL
    """)
    row = await cur.fetchone()
    if row and row[0] is not None:
        update_uniqueness_metric(TABLE_NAME, "passport_data", float(row[0]))


async def _update_duplicates(cur) -> None:
    await cur.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT last_name, first_name, middle_name, birth_date
            FROM {TABLE_NAME}
            GROUP BY last_name, first_name, middle_name, birth_date
            HAVING COUNT(*) > 1
        ) duplicates
    """)
    row = await cur.fetchone()
    update_duplicate_metric(TABLE_NAME, row[0] if row else 0)


async def _update_salary_anomalies(cur) -> None:
    await cur.execute(f"""
        WITH stats AS (
            SELECT AVG(salary) AS avg_salary, STDDEV(salary) AS stddev_salary
            FROM {TABLE_NAME}
            WHERE salary IS NOT NULL AND salary > 0
        )
        SELECT COUNT(*) FROM {TABLE_NAME}, stats
        WHERE salary > avg_salary + 3 * stddev_salary
           OR salary < avg_salary - 3 * stddev_salary
    """)
    row = await cur.fetchone()
    update_anomaly_count(TABLE_NAME, "salary", "statistical_outlier", row[0] if row else 0)


async def _update_salary_distribution(cur) -> None:
    await cur.execute(f"""
        SELECT
            CASE
                WHEN salary < 50000  THEN '0-50k'
                WHEN salary < 100000 THEN '50k-100k'
                WHEN salary < 200000 THEN '100k-200k'
                WHEN salary < 300000 THEN '200k-300k'
                ELSE '300k+'
            END AS salary_range,
            COUNT(*)
        FROM {TABLE_NAME}
        WHERE salary IS NOT NULL
        GROUP BY salary_range
    """)
    async for row in cur:
        update_salary_distribution(row[0], row[1])


async def update_quarantine_metrics() -> None:
    """Обновление размеров карантина по статусам."""
    try:
        async with get_connection() as conn:
            async with conn.cursor() as cur:
                for status in ("new", "in_review", "fixed", "rejected"):
                    await cur.execute(
                        "SELECT COUNT(*) FROM data_quality.quarantine_log WHERE dq_status = %s",
                        (status,),
                    )
                    row = await cur.fetchone()
                    update_quarantine_size(status, row[0])
    except Exception as e:
        logger.error(f"Failed to update quarantine metrics: {e}", exc_info=True)
