"""
Модуль кастомных валидаторов для кадровых данных.
Содержит бизнес-правила и проверки справочных данных.
"""

from datetime import date
from typing import Dict, Any, List, Optional, Tuple
import re
import logging
import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================
# 1. Базовые валидаторы (переиспользуемые)
# ============================================================

def validate_inn(inn: str) -> bool:
    """
    Проверка ИНН (российский формат).
    - Для физлиц: 12 цифр
    - Для юрлиц: 10 цифр
    """
    if not inn or not isinstance(inn, str):
        return False

    inn = inn.strip()

    if len(inn) not in [10, 12]:
        logger.debug(f"INN length invalid: {len(inn)}")
        return False

    if not inn.isdigit():
        logger.debug(f"INN contains non-digits: {inn}")
        return False

    if len(inn) == 12:
        coeffs_1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        coeffs_2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]

        n10 = sum(int(inn[i]) * coeffs_1[i] for i in range(10)) % 11 % 10
        n11 = sum(int(inn[i]) * coeffs_2[i] for i in range(11)) % 11 % 10

        return n10 == int(inn[10]) and n11 == int(inn[11])

    return True


def validate_snils(snils: str) -> bool:
    """
    Проверка СНИЛС (страховой номер индивидуального лицевого счёта).
    Формат: XXX-XXX-XXX YY или 11 цифр подряд.
    """
    if not snils:
        return False

    snils_clean = re.sub(r'[\s\-]', '', snils)

    if not snils_clean.isdigit() or len(snils_clean) != 11:
        return False

    total = 0
    for i, digit in enumerate(snils_clean[:9]):
        total += int(digit) * (9 - i)

    check_sum = total % 101
    if check_sum == 100:
        check_sum = 0

    return check_sum == int(snils_clean[9:])


def validate_phone(phone: str) -> bool:
    """
    Проверка российского номера телефона.
    Принимает форматы: +7XXXXXXXXXX, 8XXXXXXXXXX, 7XXXXXXXXXX, XXXXXXXXXX
    """
    if not phone:
        return False

    phone_clean = re.sub(r'[\s\-\(\)]', '', phone)

    if phone_clean.startswith('+7'):
        phone_clean = '8' + phone_clean[2:]
    elif phone_clean.startswith('7'):
        phone_clean = '8' + phone_clean[1:]

    return bool(re.match(r'^8\d{10}$', phone_clean))


def validate_email(email: str) -> bool:
    """Проверка email."""
    if not email:
        return False

    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email.strip()))


def validate_passport(series: str, number: str) -> Tuple[bool, str]:
    """Проверка паспортных данных."""
    if not series or not number:
        return False, "Паспортные данные не указаны"

    series_str = str(series).strip()
    number_str = str(number).strip()

    if not series_str.isdigit():
        return False, "Серия паспорта должна содержать только цифры"

    if not number_str.isdigit():
        return False, "Номер паспорта должен содержать только цифры"

    if len(series_str) != 4:
        return False, "Серия паспорта должна содержать 4 цифры"

    if len(number_str) != 6:
        return False, "Номер паспорта должен содержать 6 цифр"

    return True, ""


# ============================================================
# 2. Валидаторы со справочными данными
# ============================================================

class PositionValidator:
    """
    Валидатор должностей с проверкой по справочнику из БД.
    Кеширует справочник для уменьшения числа запросов.
    """

    def __init__(self, db_connection):
        self.db = db_connection
        self._cache = None
        self._cache_time = None

    def _load_positions(self) -> List[str]:
        """Загружает список допустимых должностей из БД."""
        import datetime

        if self._cache and self._cache_time and \
                (datetime.datetime.now() - self._cache_time).total_seconds() < 300:
            return self._cache

        try:
            cursor = self.db.cursor()
            cursor.execute("SELECT position_name FROM hr.dict_positions WHERE is_active = true")
            positions = [row[0] for row in cursor.fetchall()]
            cursor.close()

            self._cache = positions
            self._cache_time = datetime.datetime.now()
            logger.info(f"Loaded {len(positions)} positions from dictionary")
            return positions
        except Exception as e:
            logger.error(f"Failed to load positions: {e}")
            return []

    def validate(self, position: str) -> Tuple[bool, str]:
        """Проверяет, существует ли должность в справочнике."""
        if not position or not isinstance(position, str):
            return False, "Должность не может быть пустой"

        position_lower = position.lower().strip()
        valid_positions = [p.lower() for p in self._load_positions()]

        if position_lower not in valid_positions:
            return False, f"Должность '{position}' не найдена в справочнике"

        return True, ""


class DepartmentValidator:
    """Валидатор подразделений."""

    def __init__(self, db_connection):
        self.db = db_connection
        self._cache = None

    def _load_departments(self) -> Dict[str, int]:
        """Загружает справочник подразделений."""
        if self._cache:
            return self._cache

        cursor = self.db.cursor()
        cursor.execute("SELECT dept_name, dept_id FROM hr.dict_departments")
        self._cache = {row[0].lower(): row[1] for row in cursor.fetchall()}
        cursor.close()

        return self._cache

    def validate(self, department: str) -> Tuple[bool, str, Optional[int]]:
        """Проверяет подразделение и возвращает его ID."""
        if not department:
            return False, "Подразделение не указано", None

        dept_lower = department.lower().strip()
        dept_map = self._load_departments()

        if dept_lower not in dept_map:
            return False, f"Подразделение '{department}' не найдено", None

        return True, "", dept_map[dept_lower]


# ============================================================
# 3. Составные валидаторы (комбинируют несколько правил)
# ============================================================

class EmployeeRecordValidator:
    """Комплексный валидатор записи сотрудника."""

    def __init__(self, db_connection):
        self.db = db_connection
        self.position_validator = PositionValidator(db_connection)
        self.department_validator = DepartmentValidator(db_connection)

    def validate_full(self, record: Dict[str, Any]) -> Tuple[bool, List[Dict[str, str]]]:
        """Полная проверка записи сотрудника."""
        errors = []

        # 1. Проверка ФИО
        last_name = record.get('last_name', '')
        if not last_name or len(str(last_name).strip()) < 2:
            errors.append({"field": "last_name", "message": "Фамилия должна содержать минимум 2 символа"})
        elif not str(last_name).replace('ё', 'е').replace('-', '').isalpha():
            errors.append({"field": "last_name", "message": "Фамилия должна содержать только буквы и дефис"})

        first_name = record.get('first_name', '')
        if not first_name or len(str(first_name).strip()) < 2:
            errors.append({"field": "first_name", "message": "Имя должно содержать минимум 2 символа"})
        elif not str(first_name).replace('ё', 'е').replace('-', '').isalpha():
            errors.append({"field": "first_name", "message": "Имя должно содержать только буквы и дефис"})

        middle_name = record.get('middle_name')
        if middle_name and len(str(middle_name).strip()) < 2:
            errors.append({"field": "middle_name", "message": "Отчество должно содержать минимум 2 символа"})
        elif middle_name and not str(middle_name).replace('ё', 'е').replace('-', '').isalpha():
            errors.append({"field": "middle_name", "message": "Отчество должно содержать только буквы и дефис"})

        # 2. Проверка пола
        gender = record.get('gender', '')
        if gender and str(gender).strip() not in ['М', 'Ж']:
            errors.append({"field": "gender", "message": "Пол должен быть 'М' или 'Ж'"})

        # 3. Проверка возраста
        birth_date = record.get('birth_date')
        if birth_date:
            if isinstance(birth_date, str):
                from datetime import datetime
                try:
                    birth_date = datetime.strptime(birth_date, '%Y-%m-%d').date()
                except ValueError:
                    try:
                        birth_date = datetime.strptime(birth_date, '%d.%m.%Y').date()
                    except ValueError:
                        errors.append({"field": "birth_date", "message": "Неверный формат даты (ожидается YYYY-MM-DD)"})

            if isinstance(birth_date, date):
                today = date.today()
                age = today.year - birth_date.year - \
                      ((today.month, today.day) < (birth_date.month, birth_date.day))

                if age < 18:
                    errors.append({"field": "birth_date", "message": f"Сотруднику {age} лет (минимум 18)"})
                elif age > 100:
                    errors.append({"field": "birth_date", "message": f"Сотруднику {age} лет (максимум 100)"})

                if birth_date > today:
                    errors.append({"field": "birth_date", "message": "Дата рождения не может быть в будущем"})
        else:
            errors.append({"field": "birth_date", "message": "Дата рождения обязательна"})

        # 4. Проверка должности
        position = record.get('position', '')
        is_valid, error_msg = self.position_validator.validate(position)
        if not is_valid:
            errors.append({"field": "position", "message": error_msg})

        # 5. Проверка зарплаты
        salary = record.get('salary')
        if salary is not None:
            try:
                salary_float = float(salary)
                if salary_float < 0:
                    errors.append({"field": "salary", "message": "Зарплата не может быть отрицательной"})
                elif salary_float == 0:
                    errors.append({"field": "salary", "message": "Зарплата не может быть нулевой"})
                elif salary_float > 5_000_000:
                    errors.append({"field": "salary", "message": "Зарплата превышает допустимый лимит (5 млн)"})
                elif salary_float < 20000:
                    errors.append({"field": "salary", "message": "Зарплата ниже МРОТ"})
            except (ValueError, TypeError):
                errors.append({"field": "salary", "message": "Неверный формат зарплаты"})
        else:
            errors.append({"field": "salary", "message": "Зарплата обязательна"})

        # 6. Проверка паспортных данных
        passport_series = record.get('passport_series', '')
        passport_number = record.get('passport_number', '')
        is_valid, error_msg = validate_passport(passport_series, passport_number)
        if not is_valid and (passport_series or passport_number):
            errors.append({"field": "passport", "message": error_msg})

        # 7. Проверка дат приёма и увольнения
        hire_date = record.get('hire_date')
        termination_date = record.get('termination_date')

        if hire_date:
            if isinstance(hire_date, str):
                from datetime import datetime
                try:
                    hire_date = datetime.strptime(hire_date, '%Y-%m-%d').date()
                except ValueError:
                    try:
                        hire_date = datetime.strptime(hire_date, '%d.%m.%Y').date()
                    except ValueError:
                        errors.append({"field": "hire_date", "message": "Неверный формат даты приёма"})

            if isinstance(hire_date, date):
                if hire_date > date.today():
                    errors.append({"field": "hire_date", "message": "Дата приёма не может быть в будущем"})

                if isinstance(birth_date, date) and hire_date < birth_date:
                    errors.append({"field": "hire_date", "message": "Дата приёма не может быть раньше даты рождения"})

        if termination_date:
            if isinstance(termination_date, str):
                from datetime import datetime
                try:
                    termination_date = datetime.strptime(termination_date, '%Y-%m-%d').date()
                except ValueError:
                    try:
                        termination_date = datetime.strptime(termination_date, '%d.%m.%Y').date()
                    except ValueError:
                        errors.append({"field": "termination_date", "message": "Неверный формат даты увольнения"})

            if isinstance(termination_date, date):
                if termination_date > date.today():
                    errors.append({"field": "termination_date", "message": "Дата увольнения не может быть в будущем"})

                if isinstance(hire_date, date) and termination_date < hire_date:
                    errors.append(
                        {"field": "termination_date", "message": "Дата увольнения не может быть раньше даты приёма"})

        return len(errors) == 0, errors


# ============================================================
# 4. Вспомогательные функции для валидации Excel-столбцов
# ============================================================

def validate_excel_structure(df_columns: List[str], expected_columns: List[str]) -> Tuple[bool, List[str]]:
    """Проверяет, что Excel-файл содержит ожидаемые столбцы."""
    df_columns_set = set(df_columns)
    expected_set = set(expected_columns)

    missing = expected_set - df_columns_set
    extra = df_columns_set - expected_set

    errors = []
    if missing:
        errors.append(f"Отсутствуют столбцы: {', '.join(missing)}")
    if extra:
        errors.append(f"Лишние столбцы: {', '.join(extra)}")

    return len(errors) == 0, errors


def sanitize_dataframe(df) -> pd.DataFrame:
    """
    Очистка DataFrame перед валидацией:
    - Удаление пустых строк
    - Нормализация пробелов в строковых колонках
    - Преобразование пустых строк в None
    """
    # Удаляем строки, где все значения пустые
    df = df.dropna(how='all')

    # Для каждой колонки применяем соответствующую очистку
    for col in df.columns:
        # Проверяем, является ли колонка датой по названию
        is_date_column = 'date' in col.lower() or col in ['birth_date', 'hire_date', 'termination_date']

        if is_date_column:
            # Пробуем преобразовать в datetime
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            except:
                pass

        elif df[col].dtype == 'object':
            # Заменяем NaN на None
            df[col] = df[col].where(pd.notnull(df[col]), None)

            # Применяем строковые операции только к не-None значениям
            mask = df[col].notna()
            if mask.any():
                df.loc[mask, col] = df.loc[mask, col].astype(str).str.strip()

            # Заменяем пустые строки и специальные значения на None
            df[col] = df[col].replace({'': None, 'nan': None, 'None': None, 'null': None, 'NaN': None})

        elif df[col].dtype in ['float64', 'int64']:
            # Для числовых колонок просто оставляем как есть
            pass

    return df