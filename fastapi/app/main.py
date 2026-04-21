import io
import json
import logging
import asyncio

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Response, HTTPException
from prometheus_client import generate_latest
from apscheduler.schedulers.background import BackgroundScheduler

from database import get_db_connection
from validators import EmployeeRecordValidator, sanitize_dataframe
from metrics import (
    record_validation_result, record_validation_error, ValidationTimer,
    update_quarantine_metrics, init_metrics, record_file_processed,
    update_overall_score, update_completeness_metric,
    update_uniqueness_metric, update_freshness_metric,
    update_anomaly_count, update_active_rules, record_batch_size,
    update_table_size_metric, update_last_load_timestamp,
    update_duplicate_metric, update_salary_distribution
)

logger = logging.getLogger(__name__)
app = FastAPI()


# При старте сервиса
@app.on_event("startup")
async def startup_event():
    init_metrics(service_version="1.0.0", environment="development")

    # Инициализация активных правил валидации
    init_validation_rules()

    # Запускаем фоновый scheduler для обновления метрик
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_all_quality_metrics, 'interval', minutes=5)
    scheduler.add_job(lambda: asyncio.run(update_quarantine_metrics_async()), 'interval', minutes=1)
    scheduler.start()

    logger.info("DQ Service started")


def init_validation_rules():
    """Инициализация счетчиков активных правил валидации"""
    rules = {
        "format": 7,  # ФИО, паспорт, телефон, ИНН, email и т.д.
        "business": 5,  # Возраст, зарплата, даты приема/увольнения
        "reference": 2  # Справочник должностей, подразделений
    }
    for category, count in rules.items():
        update_active_rules(category, count)
    logger.info(f"Validation rules initialized: {rules}")


async def update_quarantine_metrics_async():
    """Асинхронная обертка для обновления метрик карантина"""
    try:
        conn = get_db_connection()
        update_quarantine_metrics(conn)
        conn.close()
    except Exception as e:
        logger.error(f"Failed to update quarantine metrics: {e}")


def update_all_quality_metrics():
    """Периодическое обновление всех метрик качества"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # 1. Общий размер таблицы
        cur.execute("SELECT COUNT(*) FROM hr.employees")
        total_count = cur.fetchone()[0]
        update_table_size_metric("hr.employees", total_count)

        # 2. Полнота данных по колонкам
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN last_name IS NOT NULL AND first_name IS NOT NULL THEN 1 ELSE 0 END) as full_name_filled,
                SUM(CASE WHEN position IS NOT NULL THEN 1 ELSE 0 END) as position_filled,
                SUM(CASE WHEN salary IS NOT NULL THEN 1 ELSE 0 END) as salary_filled,
                SUM(CASE WHEN birth_date IS NOT NULL THEN 1 ELSE 0 END) as birth_date_filled,
                SUM(CASE WHEN hire_date IS NOT NULL THEN 1 ELSE 0 END) as hire_date_filled,
                SUM(CASE WHEN passport_data IS NOT NULL THEN 1 ELSE 0 END) as passport_filled
            FROM hr.employees
        """)
        result = cur.fetchone()

        if result and result[0] > 0:
            total = result[0]

            # Обновление полноты по каждой колонке
            completeness_map = {
                "full_name": result[1],
                "position": result[2],
                "salary": result[3],
                "birth_date": result[4],
                "hire_date": result[5],
                "passport_data": result[6]
            }

            for column, filled in completeness_map.items():
                completeness_pct = (filled / total) * 100
                update_completeness_metric("hr.employees", column, completeness_pct)

            # Средняя оценка качества
            weights = {
                "full_name": 0.25,
                "position": 0.15,
                "salary": 0.15,
                "birth_date": 0.20,
                "hire_date": 0.15,
                "passport_data": 0.10
            }

            overall_score = sum(
                weights.get(col, 0) * (completeness_map.get(col, 0) / total * 100)
                for col in completeness_map.keys()
            )
            update_overall_score("hr.employees", overall_score)

        # 3. Свежесть данных (часы с последнего обновления)
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at)))/3600 as hours
            FROM hr.employees
        """)
        result = cur.fetchone()
        if result and result[0] is not None:
            update_freshness_metric("hr.employees", result[0])

        # 4. Уникальность паспортных данных
        cur.execute("""
            SELECT 
                COUNT(DISTINCT passport_data)::float / NULLIF(COUNT(*), 0)::float * 100 as uniqueness
            FROM hr.employees
            WHERE passport_data IS NOT NULL
        """)
        result = cur.fetchone()
        if result and result[0] is not None:
            update_uniqueness_metric("hr.employees", "passport_data", result[0])

        # 5. Поиск дубликатов
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT last_name, first_name, middle_name, birth_date
                FROM hr.employees
                GROUP BY last_name, first_name, middle_name, birth_date
                HAVING COUNT(*) > 1
            ) duplicates
        """)
        duplicate_groups = cur.fetchone()[0]
        update_duplicate_metric("hr.employees", duplicate_groups)

        # 6. Поиск аномалий в зарплатах
        cur.execute("""
            WITH stats AS (
                SELECT 
                    AVG(salary) as avg_salary,
                    STDDEV(salary) as stddev_salary
                FROM hr.employees
                WHERE salary IS NOT NULL AND salary > 0
            )
            SELECT COUNT(*) FROM hr.employees, stats
            WHERE salary > avg_salary + 3 * stddev_salary
               OR salary < avg_salary - 3 * stddev_salary
        """)
        anomaly_count = cur.fetchone()[0]
        update_anomaly_count("hr.employees", "salary", "statistical_outlier", anomaly_count)

        # 7. Обновление распределения зарплат
        cur.execute("""
            SELECT 
                CASE 
                    WHEN salary < 50000 THEN '0-50k'
                    WHEN salary < 100000 THEN '50k-100k'
                    WHEN salary < 200000 THEN '100k-200k'
                    WHEN salary < 300000 THEN '200k-300k'
                    ELSE '300k+'
                END as salary_range,
                COUNT(*)
            FROM hr.employees
            WHERE salary IS NOT NULL
            GROUP BY salary_range
        """)
        for range_name, count in cur.fetchall():
            update_salary_distribution(range_name, count)

        logger.info("All quality metrics updated successfully")

    except Exception as e:
        logger.error(f"Error updating quality metrics: {e}")
    finally:
        cur.close()
        conn.close()


async def run_in_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


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

        # Читаем Excel без parse_dates
        df = await run_in_thread(pd.read_excel, io.BytesIO(contents))

        # После чтения преобразуем колонки с датами, которые есть в файле
        date_columns = ['birth_date', 'hire_date', 'termination_date']
        for col in date_columns:
            if col in df.columns:
                try:
                    # Пробуем преобразовать в datetime с автоопределением формата
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                except Exception as e:
                    logger.warning(f"Could not parse dates in column {col}: {e}")

    # Записываем размер пакета в метрику
    record_batch_size("excel", len(df))

    df_columns = df.columns.tolist()

    if 'last_name' in df_columns and 'first_name' in df_columns:
        logger.info("Файл содержит отдельные колонки для ФИО")
        if 'middle_name' not in df_columns:
            df['middle_name'] = None

    # Преобразуем passport_series и passport_number в строки
    for col in ['passport_series', 'passport_number']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('.0', '', regex=False)

    # Проверяем наличие обязательных столбцов
    required_cols = ['last_name', 'first_name', 'birth_date', 'hire_date', 'position', 'salary']
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        record_file_processed("excel", success=False)
        raise HTTPException(400, detail=f"Отсутствуют обязательные столбцы: {', '.join(missing_cols)}")

    # Логируем дополнительные столбцы
    expected_cols = required_cols + ['middle_name', 'passport_series', 'passport_number', 'gender', 'termination_date']
    extra_cols = [col for col in df.columns if col not in expected_cols]
    if extra_cols:
        logger.info(f"Дополнительные столбцы в файле: {extra_cols}")

    df = await run_in_thread(sanitize_dataframe, df)
    validator = EmployeeRecordValidator(get_db_connection())

    results = {"total": len(df), "approved": 0, "quarantine": 0, "errors_detail": []}
    approved_rows, quarantine_rows = [], []

    for idx, row in df.iterrows():
        row_dict = row.to_dict()

        # Преобразуем Timestamp и datetime в date объекты
        for date_field in ['birth_date', 'hire_date', 'termination_date']:
            if date_field in row_dict and pd.notna(row_dict[date_field]):
                value = row_dict[date_field]

                # Преобразуем в date если это datetime или Timestamp
                if hasattr(value, 'date'):
                    row_dict[date_field] = value.date()
                elif hasattr(value, 'to_pydatetime'):
                    row_dict[date_field] = value.to_pydatetime().date()
                elif isinstance(value, str):
                    # Если осталась строка, пробуем распарсить
                    row_dict[date_field] = normalize_date_string(value)
            elif date_field in row_dict:
                # Если значение пустое - устанавливаем None
                row_dict[date_field] = None

        # Проверяем, что обязательные поля не пустые
        if not row_dict.get('last_name') or not row_dict.get('first_name'):
            errors = [{"field": "full_name", "message": "Фамилия и имя обязательны"}]
            quarantine_rows.append((row_dict, errors))
            record_validation_result(status="quarantine", source="excel")
            results["quarantine"] += 1
            results["errors_detail"].append({"row": idx + 2, "errors": errors})
            for err in errors:
                record_validation_error(error_type=f"{err['field']}_missing", field=err["field"])
            continue

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

    # Обновляем timestamp последней загрузки
    update_last_load_timestamp("hr.employees")

    record_file_processed("excel", success=True)

    # Обновляем метрики качества после загрузки
    await run_in_thread(update_all_quality_metrics)

    return results


def normalize_date_string(date_str: str):
    """Нормализация строки с датой в объект date"""
    from datetime import datetime, date

    if not date_str or pd.isna(date_str):
        return None

    if isinstance(date_str, (date, datetime)):
        return date_str.date() if isinstance(date_str, datetime) else date_str

    date_str = str(date_str).strip()

    formats_to_try = [
        '%Y-%m-%d',  # 2024-01-15
        '%d.%m.%Y',  # 15.01.2024
        '%d/%m/%Y',  # 15/01/2024
        '%Y.%m.%d',  # 2024.01.15
        '%d-%m-%Y',  # 15-01-2024
        '%Y%m%d',  # 20240115
        '%d%m%Y',  # 15012024
    ]

    for fmt in formats_to_try:
        try:
            parsed = datetime.strptime(date_str, fmt)
            return parsed.date()
        except ValueError:
            continue

    logger.warning(f"Could not parse date string: {date_str}")
    return None


def _batch_save_production(records: list[dict]):
    """Пакетное сохранение записей в production таблицу"""
    if not records:
        return

    conn = get_db_connection()
    cur = conn.cursor()

    def normalize_date(date_value):
        """Преобразует дату в формат YYYY-MM-DD для PostgreSQL"""
        from datetime import datetime, date

        # Если значение отсутствует
        if date_value is None or pd.isna(date_value):
            return None

        # Если уже date объект
        if isinstance(date_value, date):
            return date_value.isoformat()

        # Если datetime объект
        if isinstance(date_value, datetime):
            return date_value.date().isoformat()

        # Если pandas Timestamp
        if hasattr(date_value, 'date') and callable(getattr(date_value, 'date')):
            return date_value.date().isoformat()

        # Если строка
        if isinstance(date_value, str):
            date_str = date_value.strip()

            # Если строка пустая
            if not date_str:
                return None

            formats_to_try = [
                '%Y-%m-%d',  # 2024-01-15
                '%d.%m.%Y',  # 15.01.2024
                '%d/%m/%Y',  # 15/01/2024
                '%Y.%m.%d',  # 2024.01.15
                '%d-%m-%Y',  # 15-01-2024
            ]

            for fmt in formats_to_try:
                try:
                    parsed = datetime.strptime(date_str, fmt)
                    return parsed.date().isoformat()
                except ValueError:
                    continue

            logger.warning(f"Could not parse date: {date_value}")
            return None

        return None

    try:
        saved_count = 0
        skipped_count = 0

        for record in records:
            # Формируем passport_data
            passport_series = str(record.get('passport_series', '') or '')
            passport_number = str(record.get('passport_number', '') or '')
            passport_data = f"{passport_series} {passport_number}".strip()
            if passport_data == '' or passport_data == ' ':
                passport_data = None

            # Нормализуем даты
            birth_date = normalize_date(record.get('birth_date'))
            hire_date = normalize_date(record.get('hire_date'))
            termination_date = normalize_date(record.get('termination_date'))  # Может быть None

            # Проверяем, что обязательные даты валидные
            if not birth_date:
                logger.warning(f"Skipping record due to invalid birth_date: {record.get('birth_date')}")
                skipped_count += 1
                continue

            if not hire_date:
                logger.warning(f"Skipping record due to invalid hire_date: {record.get('hire_date')}")
                skipped_count += 1
                continue

            # Проверяем salary
            salary = record.get('salary')
            if salary is None or pd.isna(salary):
                salary = 0

            # Проверяем middle_name
            middle_name = record.get('middle_name')
            if middle_name and isinstance(middle_name, str):
                middle_name = middle_name.strip()
                if not middle_name:
                    middle_name = None

            cur.execute("""
                INSERT INTO hr.employees (
                    last_name, first_name, middle_name,
                    birth_date, hire_date, termination_date, 
                    position, salary, passport_data, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                record.get('last_name'),
                record.get('first_name'),
                middle_name,
                birth_date,
                hire_date,
                termination_date,  # Может быть NULL
                record.get('position'),
                salary,
                passport_data
            ))
            saved_count += 1

        conn.commit()
        logger.info(f"Successfully saved {saved_count} records to production (skipped: {skipped_count})")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving to production: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        cur.close()
        conn.close()


def _batch_save_quarantine(records: list[tuple[dict, list]]):
    """Пакетное сохранение записей в карантин"""
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
                elif hasattr(value, 'isoformat'):
                    clean_row[key] = value.isoformat()
                elif isinstance(value, (int, float, str, bool)):
                    clean_row[key] = value
                else:
                    clean_row[key] = str(value)

            cur.execute("""
                INSERT INTO data_quality.quarantine_log (raw_data, validation_errors, created_at)
                VALUES (%s, %s, NOW())
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


@app.get("/health")
async def health_check():
    """Endpoint для проверки здоровья сервиса"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": str(e)}
