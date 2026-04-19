import json
import logging
import asyncio

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Response, HTTPException
from prometheus_client import generate_latest

from database import get_db_connection
from validators import EmployeeRecordValidator, validate_excel_structure, sanitize_dataframe
from metrics import (
    record_validation_result, record_validation_error, ValidationTimer,
    update_quarantine_metrics, init_metrics, record_file_processed
)

logger = logging.getLogger(__name__)
app = FastAPI()


# При старте сервиса
@app.on_event("startup")
async def startup_event():
    init_metrics(service_version="1.0.0", environment="development")
    logger.info("DQ Service started")


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
        df = await run_in_thread(pd.read_excel, contents)

    expected_cols = ['full_name', 'birth_date', 'hire_date', 'position', 'salary']
    is_valid_structure, errors = validate_excel_structure(df.columns.tolist(), expected_cols)
    if not is_valid_structure:
        record_file_processed("excel", success=False)
        raise HTTPException(400, detail=errors)

    df = await run_in_thread(sanitize_dataframe, df)
    validator = EmployeeRecordValidator(get_db_connection())

    results = {"total": len(df), "approved": 0, "quarantine": 0, "errors_detail": []}
    approved_rows, quarantine_rows = [], []

    for _, row in df.iterrows():
        row_dict = row.to_dict()
        is_valid, errors = validator.validate_full(row_dict)

        if is_valid:
            approved_rows.append(row_dict)
            record_validation_result(status="approved", source="excel")
            results["approved"] += 1
        else:
            quarantine_rows.append((row_dict, errors))
            record_validation_result(status="quarantine", source="excel")
            for err in errors:
                record_validation_error(error_type=f"{err['field']}_invalid", field=err["field"])

        # Батчевая запись в БД (одно соединение на запрос)
    if approved_rows:
        await run_in_thread(_batch_save_production, approved_rows)
    if quarantine_rows:
        await run_in_thread(_batch_save_quarantine, quarantine_rows)

    record_file_processed("excel", success=True)
    await run_in_thread(update_quarantine_metrics, get_db_connection())
    return results


def _batch_save_production(records: list[dict]):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO hr.employees (full_name, birth_date, hire_date, termination_date, position, salary, passport_data)
        VALUES (%(full_name)s, %(birth_date)s, %(hire_date)s, %(termination_date)s, %(position)s, %(salary)s, 
                %(passport_series)s || ' ' || %(passport_number)s)
    """, records)
    conn.commit()
    cur.close()
    conn.close()


def _batch_save_quarantine(records: list[tuple[dict, list]]):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO data_quality.quarantine_log (raw_data, validation_errors)
        VALUES (%s, %s)
    """, [(json.dumps(r, default=str), json.dumps(e)) for r, e in records])
    conn.commit()
    cur.close()
    conn.close()


@app.get("/metrics")
async def get_metrics():
    return Response(generate_latest(), media_type="text/plain")
