from pydantic import BaseModel, field_validator, model_validator
from datetime import date
from typing import Optional


class EmployeeContract(BaseModel):
    last_name: str
    first_name: str
    middle_name: Optional[str] = None

    birth_date: date
    hire_date: date
    termination_date: Optional[date] = None
    position: str
    salary: float
    passport_series: Optional[str] = None
    passport_number: Optional[str] = None
    gender: Optional[str] = None

    @field_validator('last_name', 'first_name')
    @classmethod
    def not_empty(cls, v: str, info) -> str:
        if not v or not v.strip():
            raise ValueError(f'{info.field_name} не может быть пустым')
        if len(v.strip()) < 2:
            raise ValueError(f'{info.field_name} должен содержать минимум 2 символа')
        return v.strip()

    @field_validator('middle_name')
    @classmethod
    def validate_middle_name(cls, v: Optional[str]) -> Optional[str]:
        if v:
            return v.strip()
        return None

    @field_validator('birth_date')
    @classmethod
    def validate_age(cls, v: date) -> date:
        today = date.today()
        age = today.year - v.year - ((today.month, today.day) < (v.month, v.day))

        if v > today:
            raise ValueError('Дата рождения не может быть в будущем')
        if not (18 <= age <= 100):
            raise ValueError(f'Возраст должен быть 18-100 лет (сейчас {age})')
        return v

    @field_validator('hire_date')
    @classmethod
    def validate_hire_date(cls, v: date) -> date:
        if v > date.today():
            raise ValueError('Дата приёма не может быть в будущем')
        return v

    @field_validator('gender')
    @classmethod
    def validate_gender(cls, v: Optional[str]) -> Optional[str]:
        if v and v not in ['М', 'Ж']:
            raise ValueError('Пол должен быть М или Ж')
        return v

    @field_validator('salary')
    @classmethod
    def validate_salary(cls, v: float) -> float:
        if v < 0:
            raise ValueError('Зарплата не может быть отрицательной')
        if v == 0:
            raise ValueError('Зарплата не может быть нулевой')
        if v > 5_000_000:
            raise ValueError('Зарплата превышает допустимый лимит')
        return v

    @model_validator(mode='after')
    def validate_dates_consistency(self):
        if self.hire_date < self.birth_date:
            raise ValueError('Дата приёма не может быть раньше даты рождения')

        if self.termination_date:
            if self.termination_date > date.today():
                raise ValueError('Дата увольнения не может быть в будущем')
            if self.termination_date < self.hire_date:
                raise ValueError('Дата увольнения не может быть раньше даты приёма')

        return self
