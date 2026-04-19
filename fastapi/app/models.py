from pydantic import BaseModel, validator
from datetime import date
from typing import Optional


class EmployeeContract(BaseModel):
    full_name: str
    birth_date: date
    hire_date: date
    termination_date: Optional[date] = None
    position: str
    salary: float
    passport_series: str
    passport_number: str
    gender: str

    @validator('birth_date')
    def validate_age(cls, v):
        today = date.today()
        age = today.year - v.year - ((today.month, today.day) < (v.month, v.day))
        if not (18 <= age <= 100):
            raise ValueError(f'Возраст должен быть 18-100 лет (сейчас {age})')
        return v

    @validator('hire_date')
    def not_future(cls, v):
        if v > date.today():
            raise ValueError('Дата приёма не может быть в будущем')
        return v

    @validator('gender')
    def valid_gender(cls, v):
        if v not in ['М', 'Ж']:
            raise ValueError('Пол должен быть М или Ж')
        return v
