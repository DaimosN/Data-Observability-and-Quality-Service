"""
Базовые валидаторы и справочная валидация.
Форматная и бизнес-валидация вынесены в models.EmployeeContract.
"""
import re
import asyncio
import logging
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
from psycopg import AsyncConnection

logger = logging.getLogger(__name__)


# ============================================================
# 1. Базовые функции валидации форматов
# ============================================================

def validate_inn(inn: str) -> bool:
    """Проверка ИНН (10 или 12 цифр) с контролем контрольной суммы."""
    if not inn or not isinstance(inn, str):
        return False
    inn = inn.strip()
    if len(inn) not in (10, 12) or not inn.isdigit():
        return False

    if len(inn) == 12:
        coeffs_1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        coeffs_2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        n10 = sum(int(inn[i]) * coeffs_1[i] for i in range(10)) % 11 % 10
        n11 = sum(int(inn[i]) * coeffs_2[i] for i in range(11)) % 11 % 10
        return n10 == int(inn[10]) and n11 == int(inn[11])

    # 10 цифр — юрлицо
    coeffs = [2, 4, 10, 3, 5, 9, 4, 6, 8]
    n10 = sum(int(inn[i]) * coeffs[i] for i in range(9)) % 11 % 10
    return n10 == int(inn[9])


def validate_snils(snils: str) -> bool:
    """Проверка СНИЛС с контрольной суммой."""
    if not snils:
        return False
    clean = re.sub(r"[\s\-]", "", str(snils))
    if not clean.isdigit() or len(clean) != 11:
        return False
    total = sum(int(d) * (9 - i) for i, d in enumerate(clean[:9]))
    check_sum = total % 101
    if check_sum == 100:
        check_sum = 0
    return check_sum == int(clean[9:])


def validate_phone(phone: str) -> bool:
    """Российский номер телефона в форматах +7XXXXXXXXXX, 8XXXXXXXXXX, 7XXXXXXXXXX."""
    if not phone:
        return False
    clean = re.sub(r"[\s\-()]", "", str(phone))
    if clean.startswith("+7"):
        clean = "8" + clean[2:]
    elif clean.startswith("7") and len(clean) == 11:
        clean = "8" + clean[1:]
    return bool(re.fullmatch(r"8\d{10}", clean))


# ============================================================
# 2. Справочные валидаторы с потокобезопасным TTL-кешем
# ============================================================

class PositionValidator:
    """Проверка должностей по справочнику hr.dict_positions."""

    CACHE_TTL_SECONDS = 300  # 5 минут

    def __init__(self):
        self._cache: set[str] = set()
        self._cache_at: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def _refresh(self, conn: AsyncConnection) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT position_name FROM hr.dict_positions WHERE is_active = true"
            )
            rows = await cur.fetchall()
        self._cache = {row[0].lower() for row in rows}
        self._cache_at = datetime.now()
        logger.info(f"Positions cache refreshed: {len(self._cache)} items")

    async def _ensure_fresh(self, conn: AsyncConnection) -> None:
        async with self._lock:
            is_stale = (
                    self._cache_at is None
                    or (datetime.now() - self._cache_at).total_seconds() > self.CACHE_TTL_SECONDS
            )
            if is_stale:
                await self._refresh(conn)

    async def validate(self, conn: AsyncConnection, position: str) -> Tuple[bool, str]:
        if not position or not isinstance(position, str):
            return False, "Должность не указана"
        await self._ensure_fresh(conn)
        if position.lower().strip() not in self._cache:
            return False, f"Должность '{position}' не найдена в справочнике"
        return True, ""


class DepartmentValidator:
    """Проверка подразделений по справочнику hr.dict_departments."""
    """В РАЗРАБОТКЕ"""
    CACHE_TTL_SECONDS = 3600  # 1 час

    def __init__(self):
        self._cache: dict[str, int] = {}
        self._cache_at: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def _refresh(self, conn: AsyncConnection) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT dept_name, dept_id FROM hr.dict_departments WHERE is_active = true"
            )
            rows = await cur.fetchall()
        self._cache = {row[0].lower(): row[1] for row in rows}
        self._cache_at = datetime.now()
        logger.info(f"Departments cache refreshed: {len(self._cache)} items")

    async def validate(
            self, conn: AsyncConnection, department: str
    ) -> Tuple[bool, str, Optional[int]]:
        if not department:
            return False, "Подразделение не указано", None
        async with self._lock:
            is_stale = (
                    self._cache_at is None
                    or (datetime.now() - self._cache_at).total_seconds() > self.CACHE_TTL_SECONDS
            )
            if is_stale:
                await self._refresh(conn)
        key = department.lower().strip()
        if key not in self._cache:
            return False, f"Подразделение '{department}' не найдено", None
        return True, "", self._cache[key]


# ============================================================
# 3. Утилиты подготовки DataFrame
# ============================================================

def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Очистка DataFrame перед валидацией:
      - удаление полностью пустых строк;
      - нормализация пробелов в строковых колонках;
      - замена пустых строк и NaN на None.
    """
    df = df.dropna(how="all")

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].where(pd.notnull(df[col]), None)
            mask = df[col].notna()
            if mask.any():
                df.loc[mask, col] = df.loc[mask, col].astype(str).str.strip()
            df[col] = df[col].replace(
                {"": None, "nan": None, "None": None, "null": None, "NaN": None}
            )
    return df


def validate_excel_structure(
        df_columns: list[str], required_columns: list[str]
) -> Tuple[bool, list[str]]:
    """Проверяет наличие обязательных колонок в Excel-файле."""
    missing = set(required_columns) - set(df_columns)
    if missing:
        return False, [f"Отсутствуют обязательные столбцы: {', '.join(sorted(missing))}"]
    return True, []
