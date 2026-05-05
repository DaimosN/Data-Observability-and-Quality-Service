"""
FastAPI приложение Data Quality Service.
"""
import io
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Annotated

import pandas as pd
from fastapi import Depends, FastAPI, File, HTTPException, Response, UploadFile
from pydantic import ValidationError
from psycopg import AsyncConnection
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from prometheus_client import generate_latest

from database import init_pool, close_pool, db_dependency
from models import EmployeeContract
from validators import (
    PositionValidator,
    sanitize_dataframe,
    validate_excel_structure,
)
from batch_save import batch_save_production, batch_save_quarantine
from quality_metrics import update_all_quality_metrics, update_quarantine_metrics
from metrics import (
    init_metrics,
    record_validation_result,
    record_validation_error,
    record_file_processed,
    record_batch_size,
    update_last_load_timestamp,
    ValidationTimer,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Константы
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50 МБ
REQUIRED_COLUMNS = [
    "last_name", "first_name", "birth_date", "hire_date", "position", "salary",
]

# Глобальные синглтоны
position_validator = PositionValidator()


# ============================================================
# Lifespan
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    await init_pool()
    init_metrics(service_version="1.0.0", environment="development")

    scheduler = AsyncIOScheduler()
    scheduler.add_job(update_all_quality_metrics, "interval", minutes=5)
    scheduler.add_job(update_quarantine_metrics, "interval", minutes=1)
    scheduler.start()
    app.state.scheduler = scheduler

    logger.info("DQ Service started")
    try:
        yield
    finally:
        # --- shutdown ---
        app.state.scheduler.shutdown(wait=True)
        await close_pool()
        logger.info("DQ Service stopped")


app = FastAPI(
    title="Data Quality Service",
    version="1.0.0",
    description="Сервис контроля качества кадровых данных",
    lifespan=lifespan,
)


# ============================================================
# Эндпоинты
# ============================================================

@app.post("/upload/excel/")
async def upload_excel(
    file: UploadFile = File(...),
    conn: AsyncConnection = Depends(db_dependency),
):
    """Приём Excel-файла, валидация и маршрутизация записей (production/карантин)."""
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(400, "Поддерживаются только .xlsx файлы")

    contents = await file.read()
    if len(contents) > MAX_UPLOAD_SIZE:
        raise HTTPException(413, f"Файл превышает {MAX_UPLOAD_SIZE // 1024 // 1024} МБ")

    # --- Чтение Excel ---
    try:
        with ValidationTimer("excel_processing"):
            df = await asyncio.to_thread(pd.read_excel, io.BytesIO(contents))
            df = await asyncio.to_thread(sanitize_dataframe, df)
    except Exception as e:
        record_file_processed("excel", success=False)
        logger.error(f"Failed to read Excel: {e}", exc_info=True)
        raise HTTPException(400, f"Ошибка чтения Excel: {e}")

    record_batch_size("excel", len(df))

    # Проверка структуры файла
    ok, errors = validate_excel_structure(df.columns.tolist(), REQUIRED_COLUMNS)
    if not ok:
        record_file_processed("excel", success=False)
        raise HTTPException(400, "; ".join(errors))

    if "middle_name" not in df.columns:
        df["middle_name"] = None

    # --- Валидация построчно ---
    results = {
        "total": len(df),
        "approved": 0,
        "quarantine": 0,
        "errors_detail": [],
    }
    approved_rows: list[dict] = []
    quarantine_rows: list[tuple[dict, list]] = []

    with ValidationTimer("batch"):
        for idx, row in df.iterrows():
            row_dict = row.where(pd.notna(row), None).to_dict()
            row_errors: list[dict] = []
            contract: EmployeeContract | None = None

            # Шаг 1: форматная и бизнес-валидация (Pydantic)
            try:
                contract = EmployeeContract.model_validate(row_dict)
            except ValidationError as e:
                for err in e.errors():
                    row_errors.append({
                        "field": ".".join(str(x) for x in err["loc"]),
                        "message": err["msg"],
                    })

            # Шаг 2: справочная валидация (только если Pydantic прошёл)
            if contract is not None:
                ok_pos, msg_pos = await position_validator.validate(conn, contract.position)
                if not ok_pos:
                    row_errors.append({"field": "position", "message": msg_pos})

            # Шаг 3: маршрутизация
            if not row_errors and contract is not None:
                approved_rows.append(contract.model_dump())
                record_validation_result(status="approved", source="excel")
                results["approved"] += 1
            else:
                quarantine_rows.append((row_dict, row_errors))
                record_validation_result(status="quarantine", source="excel")
                results["quarantine"] += 1
                results["errors_detail"].append({
                    "row": idx + 2,
                    "errors": row_errors,
                })
                for err in row_errors:
                    record_validation_error(
                        error_type=f"{err['field']}_invalid",
                        field=err["field"],
                    )

    # --- Batch-сохранение ---
    try:
        await batch_save_production(approved_rows)
        await batch_save_quarantine(quarantine_rows)
    except Exception as e:
        record_file_processed("excel", success=False)
        logger.error(f"Failed to persist records: {e}", exc_info=True)
        raise HTTPException(500, f"Ошибка сохранения: {e}")

    update_last_load_timestamp("hr.employees")
    record_file_processed("excel", success=True)

    # Пересчёт метрик качества после загрузки (в фоне)
    asyncio.create_task(update_all_quality_metrics())

    return results


@app.get("/metrics")
async def get_metrics():
    """Prometheus-совместимый эндпоинт метрик."""
    return Response(generate_latest(), media_type="text/plain; version=0.0.4")


@app.get("/health")
async def health_check(conn: AsyncConnection = Depends(db_dependency)):
    """Проверка готовности сервиса и доступности БД."""
    try:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            await cur.fetchone()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return Response(
            content='{"status": "unhealthy"}',
            media_type="application/json",
            status_code=503,
        )
