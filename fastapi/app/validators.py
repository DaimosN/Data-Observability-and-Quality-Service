import pandas as pd

"""
Модуль кастомных валидаторов для HR-данных.
Содержит бизнес-правила и проверки справочных данных.
"""

from datetime import date
from typing import Dict, Any, List, Optional, Tuple
import re
import logging

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

    # Проверка длины
    if len(inn) not in [10, 12]:
        logger.debug(f"INN length invalid: {len(inn)}")
        return False

    # Проверка что все символы — цифры
    if not inn.isdigit():
        logger.debug(f"INN contains non-digits: {inn}")
        return False

    # Контрольные суммы (упрощённая версия)
    if len(inn) == 12:
        # Для физлиц: проверка двух последних цифр
        coeffs_1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        coeffs_2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]

        n10 = sum(int(inn[i]) * coeffs_1[i] for i in range(10)) % 11 % 10
        n11 = sum(int(inn[i]) * coeffs_2[i] for i in range(11)) % 11 % 10

        return n10 == int(inn[10]) and n11 == int(inn[11])

    return True  # Для юрлиц упрощаем


def validate_snils(snils: str) -> bool:
    """
    Проверка СНИЛС (страховой номер индивидуального лицевого счёта).
    Формат: XXX-XXX-XXX YY или 11 цифр подряд.
    """
    if not snils:
        return False

    # Очистка от разделителей
    snils_clean = re.sub(r'[\s\-]', '', snils)

    # Должно быть ровно 11 цифр
    if not snils_clean.isdigit() or len(snils_clean) != 11:
        return False

    # Контрольное число
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

    # Очистка от лишних символов
    phone_clean = re.sub(r'[\s\-\(\)]', '', phone)

    # Нормализация
    if phone_clean.startswith('+7'):
        phone_clean = '8' + phone_clean[2:]
    elif phone_clean.startswith('7'):
        phone_clean = '8' + phone_clean[1:]

    # Должно быть 11 цифр, начинаться с 8
    return bool(re.match(r'^8\d{10}$', phone_clean))


# ============================================================
# 2. Валидаторы со справочными данными (подключаются к БД)
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
        """Загружает список допустимых должностей из БД"""
        import datetime

        # Кеш на 5 минут
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
        """
        Проверяет, существует ли должность в справочнике.
        Возвращает (is_valid, error_message)
        """
        if not position or not isinstance(position, str):
            return False, "Должность не может быть пустой"

        position_lower = position.lower().strip()
        valid_positions = [p.lower() for p in self._load_positions()]

        if position_lower not in valid_positions:
            return False, f"Должность '{position}' не найдена в справочнике"

        return True, ""


class DepartmentValidator:
    """Валидатор подразделений"""

    def __init__(self, db_connection):
        self.db = db_connection
        self._cache = None

    def _load_departments(self) -> Dict[str, int]:
        """Загружает справочник подразделений"""
        if self._cache:
            return self._cache

        cursor = self.db.cursor()
        cursor.execute("SELECT dept_name, dept_id FROM hr.dict_departments")
        self._cache = {row[0].lower(): row[1] for row in cursor.fetchall()}
        cursor.close()

        return self._cache

    def validate(self, department: str) -> Tuple[bool, str, Optional[int]]:
        """
        Проверяет подразделение и возвращает его ID.
        Возвращает (is_valid, error_message, department_id)
        """
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
    """
    Комплексный валидатор записи сотрудника.
    Использует другие валидаторы для проверки всех полей.
    """

    def __init__(self, db_connection):
        self.db = db_connection
        self.position_validator = PositionValidator(db_connection)
        self.department_validator = DepartmentValidator(db_connection)

    def validate_full(self, record: Dict[str, Any]) -> Tuple[bool, List[Dict[str, str]]]:
        """
        Полная проверка записи сотрудника.

        Args:
            record: словарь с данными сотрудника

        Returns:
            (is_valid, errors_list)
            errors_list: [{"field": "birth_date", "message": "Возраст должен быть 18-100"}]
        """
        errors = []

        # 1. Проверка ФИО (не пустое, минимальная длина)
        full_name = record.get('full_name', '')
        if not full_name or len(full_name.strip()) < 2:
            errors.append({"field": "full_name", "message": "ФИО должно содержать минимум 2 символа"})

        # 2. Проверка возраста (дублирует Pydantic, но здесь можно сделать сложнее)
        birth_date = record.get('birth_date')
        if birth_date:
            if isinstance(birth_date, str):
                from datetime import datetime
                try:
                    birth_date = datetime.strptime(birth_date, '%Y-%m-%d').date()
                except ValueError:
                    errors.append({"field": "birth_date", "message": "Неверный формат даты"})

            if isinstance(birth_date, date):
                today = date.today()
                age = today.year - birth_date.year - \
                      ((today.month, today.day) < (birth_date.month, birth_date.day))

                if age < 18:
                    errors.append({"field": "birth_date", "message": f"Сотруднику {age} лет (минимум 18)"})
                elif age > 100:
                    errors.append({"field": "birth_date", "message": f"Сотруднику {age} лет (максимум 100)"})
        else:
            errors.append({"field": "birth_date", "message": "Дата рождения обязательна"})

        # 3. Проверка должности
        position = record.get('position', '')
        is_valid, error_msg = self.position_validator.validate(position)
        if not is_valid:
            errors.append({"field": "position", "message": error_msg})

        # 4. Проверка зарплаты
        salary = record.get('salary')
        if salary is not None:
            try:
                salary = float(salary)
                if salary < 0:
                    errors.append({"field": "salary", "message": "Зарплата не может быть отрицательной"})
                elif salary > 5_000_000:
                    errors.append({"field": "salary", "message": "Зарплата превышает допустимый лимит"})
            except (ValueError, TypeError):
                errors.append({"field": "salary", "message": "Неверный формат зарплаты"})

        # 5. Проверка телефона (если есть)
        phone = record.get('phone')
        if phone and not validate_phone(phone):
            errors.append({"field": "phone", "message": "Неверный формат телефона"})

        # 6. Проверка ИНН (если есть)
        inn = record.get('inn')
        if inn and not validate_inn(inn):
            errors.append({"field": "inn", "message": "Неверный формат ИНН"})

        # 7. Проверка дат приёма и увольнения
        hire_date = record.get('hire_date')
        termination_date = record.get('termination_date')

        if hire_date:
            if isinstance(hire_date, str):
                from datetime import datetime
                try:
                    hire_date = datetime.strptime(hire_date, '%Y-%m-%d').date()
                except ValueError:
                    errors.append({"field": "hire_date", "message": "Неверный формат даты приёма"})
            else:
                if hire_date > date.today():
                    errors.append({"field": "hire_date", "message": "Дата приёма не может быть в будущем"})

        if termination_date:
            if isinstance(termination_date, str):
                from datetime import datetime
                try:
                    termination_date = datetime.strptime(termination_date, '%Y-%m-%d').date()
                except ValueError:
                    errors.append({"field": "termination_date", "message": "Неверный формат даты увольнения"})
            else:
                if hire_date and termination_date < hire_date:
                    errors.append(
                        {"field": "termination_date", "message": "Дата увольнения не может быть раньше даты приёма"})

        return len(errors) == 0, errors


# ============================================================
# 4. Вспомогательные функции для валидации Excel-столбцов
# ============================================================

def validate_excel_structure(df_columns: List[str], expected_columns: List[str]) -> Tuple[bool, List[str]]:
    """
    Проверяет, что Excel-файл содержит ожидаемые столбцы.

    Returns:
        (is_valid, missing_columns)
    """
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

    # Для строковых колонок: обрезаем пробелы, пустые строки -> None
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip().replace({'nan': None, '': None, 'None': None})
    return df
