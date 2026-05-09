"""
Pydantic-модели для валидации кадровых данных.
Единая точка форматной и бизнес-валидации.
"""
from datetime import date, datetime
from typing import Optional, Any
import re
import pandas as pd

from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    ConfigDict,
)

# ----- регулярные выражения для переиспользования -----
_NAME_RE = re.compile(r"^[А-Яа-яЁё\- ']{2,}$")
_DIGITS_RE = re.compile(r"^\d+$")
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def _parse_date_flexible(value: Any) -> Optional[date]:
    """Универсальный парсер дат из разных форматов."""
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    # pandas.Timestamp
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime().date()
    if isinstance(value, str):
        s = value.strip()
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y.%m.%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    raise ValueError(f"Не удалось распознать дату: {value!r}")


class EmployeeContract(BaseModel):
    """
    Контракт кадровой записи.
    Отвечает за форматную и логическую валидацию.
    Справочная валидация (должность в dict_positions) вынесена в отдельный слой.
    """
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="ignore",  # лишние поля из Excel не ломают валидацию
    )

    last_name: str = Field(..., min_length=2, max_length=100)
    first_name: str = Field(..., min_length=2, max_length=100)
    middle_name: Optional[str] = Field(None, max_length=100)

    birth_date: date
    hire_date: date
    termination_date: Optional[date] = None

    position: str = Field(..., min_length=2, max_length=100)
    salary: float = Field(..., gt=0, le=5_000_000)

    passport_series: Optional[str] = None
    passport_number: Optional[str] = None
    gender: Optional[str] = None

    inn: Optional[str] = None
    snils: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None

    # ---------- Парсеры входа (mode='before') ----------
    @field_validator("birth_date", "hire_date", "termination_date", mode="before")
    @classmethod
    def _parse_dates(cls, v: Any) -> Any:
        return _parse_date_flexible(v)

    @field_validator(
        "passport_series", "passport_number", "inn", "snils",
        mode="before",
    )
    @classmethod
    def _normalize_digits_field(cls, v: Any) -> Any:
        if v is None:
            return None
        # Убираем артефакты pandas ("1234.0"), пробелы, дефисы
        # Преобразуем float/int в строку без .0
        if isinstance(v, (int, float)):
            if pd.isna(v):
                return None
            s = str(int(v))  # int, чтобы убрать .0
        else:
            s = str(v).strip().replace(" ", "").replace("-", "")
            if s.endswith(".0"):
                s = s[:-2]
        return s or None

    # ---------- Форматная валидация ----------
    @field_validator("last_name", "first_name")
    @classmethod
    def _check_name(cls, v: str, info) -> str:
        if not _NAME_RE.match(v):
            raise ValueError(
                f"{info.field_name}: допустимы только русские буквы, дефис, апостроф и пробел"
            )
        return v

    @field_validator("middle_name")
    @classmethod
    def _check_middle_name(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return None
        if not _NAME_RE.match(v):
            raise ValueError(
                "middle_name: допустимы только русские буквы, дефис, апостроф и пробел"
            )
        return v

    @field_validator("gender")
    @classmethod
    def _check_gender(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return None
        if v not in ("М", "Ж"):
            raise ValueError("gender: допустимы только 'М' или 'Ж'")
        return v

    @field_validator("passport_series")
    @classmethod
    def _check_passport_series(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        if len(v) != 4 or not _DIGITS_RE.match(v):
            raise ValueError("passport_series: 4 цифры")
        return v

    @field_validator("passport_number")
    @classmethod
    def _check_passport_number(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        if len(v) != 6 or not _DIGITS_RE.match(v):
            raise ValueError("passport_number: 6 цифр")
        return v

    @field_validator("inn")
    @classmethod
    def _check_inn(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        from validators import validate_inn  # избегаем циклического импорта
        if not validate_inn(v):
            raise ValueError("inn: неверный формат или контрольная сумма")
        return v

    @field_validator("snils")
    @classmethod
    def _check_snils(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        from validators import validate_snils
        if not validate_snils(v):
            raise ValueError("snils: неверный формат или контрольная сумма")
        return v

    @field_validator("phone")
    @classmethod
    def _check_phone(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return None
        from validators import validate_phone
        if not validate_phone(v):
            raise ValueError("phone: неверный формат российского номера")
        return v

    @field_validator("email")
    @classmethod
    def _check_email(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return None
        if not _EMAIL_RE.match(v.strip()):
            raise ValueError("email: неверный формат")
        return v.strip().lower()

    # ---------- Бизнес-валидация (кросс-полевая) ----------
    @model_validator(mode="after")
    def _check_business_rules(self):
        today = date.today()

        # Возраст
        if self.birth_date > today:
            raise ValueError("birth_date: дата рождения не может быть в будущем")
        age = today.year - self.birth_date.year - (
                (today.month, today.day) < (self.birth_date.month, self.birth_date.day)
        )
        if age < 18:
            raise ValueError(f"birth_date: сотруднику {age} лет (минимум 18)")
        if age > 100:
            raise ValueError(f"birth_date: сотруднику {age} лет (максимум 100)")

        # Даты приёма/увольнения
        if self.hire_date > today:
            raise ValueError("hire_date: дата приёма не может быть в будущем")
        if self.hire_date < self.birth_date:
            raise ValueError("hire_date: дата приёма раньше даты рождения")

        if self.termination_date is not None:
            if self.termination_date > today:
                raise ValueError("termination_date: дата увольнения не может быть в будущем")
            if self.termination_date < self.hire_date:
                raise ValueError("termination_date: дата увольнения раньше даты приёма")

        # МРОТ (нижний порог)
        if self.salary < 20_000:
            raise ValueError("salary: зарплата ниже МРОТ (20 000 ₽)")

        # Паспорт — либо оба поля, либо ни одного
        if bool(self.passport_series) != bool(self.passport_number):
            raise ValueError("passport: должны быть заполнены и серия, и номер")

        return self
