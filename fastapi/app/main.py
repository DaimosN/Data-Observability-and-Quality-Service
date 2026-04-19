import io
import json
import logging
import asyncio

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Response, HTTPException
from prometheus_client import generate_latest
from apscheduler.schedulers.background import BackgroundScheduler

from database import get_db_connection
from validators import EmployeeRecordValidator, validate_excel_structure, sanitize_dataframe
from metrics import (
    record_validation_result, record_validation_error, ValidationTimer,
    update_quarantine_metrics, init_metrics, record_file_processed, update_overall_score, update_completeness_metric,
    update_overall_score, update_completeness_metric,
    update_uniqueness_metric, update_freshness_metric
)

logger = logging.getLogger(__name__)
app = FastAPI()


# При старте сервиса
@app.on_event("startup")
async def startup_event():
    init_metrics(service_version="1.0.0", environment="development")

    # Запускаем фоновый scheduler для обновления метрик
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_all_quality_metrics, 'interval', minutes=5)
    scheduler.start()

    logger.info("DQ Service started")


def update_all_quality_metrics():
    """Периодическое обновление всех метрик качества"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # 1. Общая оценка качества
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN full_name IS NOT NULL THEN 1 ELSE 0 END) as full_name_filled,
                SUM(CASE WHEN position IS NOT NULL THEN 1 ELSE 0 END) as position_filled,
                SUM(CASE WHEN salary IS NOT NULL THEN 1 ELSE 0 END) as salary_filled
            FROM hr.employees
        """)
        result = cur.fetchone()

        if result and result[0] > 0:
            # Средняя полнота данных
            completeness_score = (
                                         (result[1] + result[2] + result[3]) / (result[0] * 3)
                                 ) * 100
            update_overall_score("hr.employees", completeness_score)

            # Полнота по колонкам
            update_completeness_metric("hr.employees", "full_name",
                                       (result[1] / result[0]) * 100 if result[0] > 0 else 0)
            update_completeness_metric("hr.employees", "position",
                                       (result[2] / result[0]) * 100 if result[0] > 0 else 0)
            update_completeness_metric("hr.employees", "salary",
                                       (result[3] / result[0]) * 100 if result[0] > 0 else 0)

        # 2. Свежесть данных (часы с последнего обновления)
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at)))/3600 as hours
            FROM hr.employees
        """)
        result = cur.fetchone()
        if result and result[0] is not None:
            update_freshness_metric("hr.employees", result[0])

        # 3. Уникальность (например, по паспортам)
        cur.execute("""
            SELECT 
                COUNT(DISTINCT passport_data)::float / COUNT(*)::float * 100 as uniqueness
            FROM hr.employees
            WHERE passport_data IS NOT NULL
        """)
        result = cur.fetchone()
        if result and result[0] is not None:
            update_uniqueness_metric("hr.employees", "passport_data", result[0])

    except Exception as e:
        logger.error(f"Error updating quality metrics: {e}")
    finally:
        cur.close()
        conn.close()


async def run_in_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


# dq_validations_total = Counter('dq_validations_total', 'Total validations', ['status'])
# dq_validation_errors = Counter('dq_validation_errors_total', 'Errors by type', ['error_type'])


def save_to_production(record):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO hr.employees (full_name, birth_date, hire_date, termination_date, position, salary, passport_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        record.full_name, record.birth_date, record.hire_date,
        record.termination_date, record.position, record.salary,
        f"{record.passport_series} {record.passport_number}"
    ))
    conn.commit()
    cur.close()
    conn.close()


def save_to_quarantine(raw_row, errors):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO data_quality.quarantine_log (raw_data, validation_errors)
        VALUES (%s, %s)
    """, (json.dumps(raw_row, default=str), json.dumps(errors)))
    conn.commit()
    cur.close()
    conn.close()


@app.post("/upload/excel/")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(400, "Поддерживаются только .xlsx файлы")

    with ValidationTimer("excel_processing"):
        contents = await file.read()
        df = await run_in_thread(pd.read_excel, io.BytesIO(contents))

    # Преобразуем passport_series и passport_number в строки
    if 'passport_series' in df.columns:
        df['passport_series'] = df['passport_series'].astype(str).str.replace('.0', '', regex=False)
    if 'passport_number' in df.columns:
        df['passport_number'] = df['passport_number'].astype(str).str.replace('.0', '', regex=False)

    # Проверяем только наличие обязательных столбцов
    required_cols = ['full_name', 'birth_date', 'hire_date', 'position', 'salary']
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        record_file_processed("excel", success=False)
        raise HTTPException(400, detail=f"Отсутствуют обязательные столбцы: {', '.join(missing_cols)}")

    # Логируем дополнительные столбцы
    extra_cols = [col for col in df.columns if col not in required_cols]
    if extra_cols:
        logger.info(f"Дополнительные столбцы в файле: {extra_cols}")

    df = await run_in_thread(sanitize_dataframe, df)
    validator = EmployeeRecordValidator(get_db_connection())

    results = {"total": len(df), "approved": 0, "quarantine": 0, "errors_detail": []}
    approved_rows, quarantine_rows = [], []

    for idx, row in df.iterrows():
        row_dict = row.to_dict()

        # Преобразуем даты из Timestamp в date если нужно
        for date_field in ['birth_date', 'hire_date', 'termination_date']:
            if date_field in row_dict and hasattr(row_dict[date_field], 'date'):
                row_dict[date_field] = row_dict[date_field].date()

        is_valid, errors = validator.validate_full(row_dict)

        if is_valid:
            approved_rows.append(row_dict)
            record_validation_result(status="approved", source="excel")
            results["approved"] += 1
        else:
            quarantine_rows.append((row_dict, errors))
            record_validation_result(status="quarantine", source="excel")
            results["quarantine"] += 1
            results["errors_detail"].append({"row": idx + 2, "errors": errors})
            for err in errors:
                record_validation_error(error_type=f"{err['field']}_invalid", field=err["field"])

    # Батчевая запись в БД
    if approved_rows:
        await run_in_thread(_batch_save_production, approved_rows)
    if quarantine_rows:
        await run_in_thread(_batch_save_quarantine, quarantine_rows)

    record_file_processed("excel", success=True)

    # Попытка обновить метрики карантина
    try:
        await run_in_thread(update_quarantine_metrics, get_db_connection())
    except Exception as e:
        logger.warning(f"Could not update quarantine metrics: {e}")

    # После сохранения данных, обновите метрики качества
    if approved_rows:
        # Рассчитываем оценку качества
        total_records = len(df)
        if total_records > 0:
            quality_score = (results["approved"] / total_records) * 100
            update_overall_score("hr.employees", quality_score)

            # Обновляем полноту данных по колонкам
            for column in ['full_name', 'position', 'salary']:
                if column in df.columns:
                    non_null_count = df[column].notna().sum()
                    completeness = (non_null_count / total_records) * 100
                    update_completeness_metric("hr.employees", column, completeness)

        return results

    return results


def _batch_save_production(records: list[dict]):
    """
    Пакетное сохранение записей в production таблицу
    """
    if not records:
        return

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        for record in records:
            # Формируем passport_data из series и number
            passport_series = str(record.get('passport_series', ''))
            passport_number = str(record.get('passport_number', ''))
            passport_data = f"{passport_series} {passport_number}".strip()
            if passport_data == '':
                passport_data = None

            # Безопасно получаем termination_date (может отсутствовать или быть None)
            termination_date = record.get('termination_date')
            if termination_date and hasattr(termination_date, 'isoformat'):
                termination_date = termination_date.isoformat()

            # Получаем даты и преобразуем если нужно
            birth_date = record.get('birth_date')
            if birth_date and hasattr(birth_date, 'isoformat'):
                birth_date = birth_date.isoformat()

            hire_date = record.get('hire_date')
            if hire_date and hasattr(hire_date, 'isoformat'):
                hire_date = hire_date.isoformat()

            cur.execute("""
                INSERT INTO hr.employees (full_name, birth_date, hire_date, termination_date, position, salary, passport_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                record.get('full_name'),
                birth_date,
                hire_date,
                termination_date,
                record.get('position'),
                record.get('salary'),
                passport_data
            ))

        conn.commit()
        logger.info(f"Successfully saved {len(records)} records to production")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving to production: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def _batch_save_quarantine(records: list[tuple[dict, list]]):
    """
    Пакетное сохранение записей в карантин
    """
    if not records:
        return

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        for raw_row, errors in records:
            # Очищаем данные для JSON сериализации
            clean_row = {}
            for key, value in raw_row.items():
                if value is None:
                    clean_row[key] = None
                elif hasattr(value, 'isoformat'):  # datetime, date
                    clean_row[key] = value.isoformat()
                elif isinstance(value, (int, float, str, bool)):
                    clean_row[key] = value
                else:
                    clean_row[key] = str(value)

            cur.execute("""
                INSERT INTO data_quality.quarantine_log (raw_data, validation_errors)
                VALUES (%s, %s)
            """, (
                json.dumps(clean_row, ensure_ascii=False, default=str),
                json.dumps(errors, ensure_ascii=False, default=str)
            ))

        conn.commit()
        logger.info(f"Successfully saved {len(records)} records to quarantine")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving to quarantine: {e}")
        raise
    finally:
        cur.close()
        conn.close()


@app.get("/metrics")
async def get_metrics():
    return Response(generate_latest(), media_type="text/plain")
