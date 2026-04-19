# test_data_generator.py
import pandas as pd
import random
from datetime import date, timedelta
from faker import Faker

fake = Faker('ru_RU')

# Константы
POSITIONS = ['инженер', 'бухгалтер', 'менеджер', 'директор', 'программист', 'аналитик', 'водитель']
GENDERS = ['М', 'Ж']
SERIES = [str(random.randint(1000, 9999)) for _ in range(100)]
NUMBERS = [str(random.randint(100000, 999999)) for _ in range(100)]


def generate_error_age():
    """Генерирует дату рождения с возможными ошибками"""
    error_types = [
        'normal',  # нормальная дата
        'future',  # дата в будущем
        'too_old',  # старше 150 лет
        'child'  # младше 18 лет для сотрудника
    ]

    error = random.choices(error_types, weights=[0.7, 0.1, 0.1, 0.1])[0]

    if error == 'normal':
        return fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat()
    elif error == 'future':
        return (date.today() + timedelta(days=random.randint(1, 365))).isoformat()
    elif error == 'too_old':
        return (date.today() - timedelta(days=random.randint(365 * 150, 365 * 200))).isoformat()
    else:  # child - младше 18
        return (date.today() - timedelta(days=random.randint(1, 365 * 17))).isoformat()


def generate_error_salary():
    """Генерирует зарплату с возможными ошибками"""
    error_types = [
        'normal',  # нормальная зарплата
        'negative',  # отрицательная
        'zero',  # нулевая
        'huge',  # слишком большая (>1 млн)
        'string'  # строка вместо числа
    ]

    error = random.choices(error_types, weights=[0.7, 0.08, 0.07, 0.1, 0.05])[0]

    if error == 'normal':
        return random.randint(25000, 300000)
    elif error == 'negative':
        return -random.randint(1000, 100000)
    elif error == 'zero':
        return 0
    elif error == 'huge':
        return random.randint(2_000_000, 10_000_000)
    else:  # string
        return f"{random.randint(10000, 500000)} руб."


def generate_error_passport():
    """Генерирует паспортные данные с ошибками"""
    error_types = [
        'normal',  # нормальный
        'short_series',  # короткая серия
        'long_series',  # длинная серия
        'short_number',  # короткий номер
        'long_number',  # длинный номер
        'letters',  # буквы в номере
        'missing'  # пропущенные значения
    ]

    error = random.choices(error_types, weights=[0.7, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05])[0]

    if error == 'normal':
        return random.choice(SERIES), random.choice(NUMBERS)
    elif error == 'short_series':
        return str(random.randint(10, 999)), random.choice(NUMBERS)
    elif error == 'long_series':
        return str(random.randint(10000, 999999)), random.choice(NUMBERS)
    elif error == 'short_number':
        return random.choice(SERIES), str(random.randint(1000, 99999))
    elif error == 'long_number':
        return random.choice(SERIES), str(random.randint(1000000, 99999999))
    elif error == 'letters':
        return random.choice(SERIES), f"{random.choice(['AB', 'CD', 'XY'])}{random.randint(1000, 999999)}"
    else:  # missing
        return None, None


def generate_error_hire_date():
    """Генерирует дату приема с возможными ошибками"""
    error_types = [
        'normal',  # нормальная дата
        'future',  # дата в будущем
        'before_birth',  # раньше рождения
        'invalid'  # неверный формат
    ]

    error = random.choices(error_types, weights=[0.8, 0.07, 0.1, 0.03])[0]

    if error == 'normal':
        return fake.date_between(start_date='-10y', end_date='today').isoformat()
    elif error == 'future':
        return (date.today() + timedelta(days=random.randint(1, 365))).isoformat()
    elif error == 'invalid':
        return f"{random.randint(1, 31)}/{random.randint(1, 12)}/{random.randint(2020, 2025)}"
    else:  # before_birth - будет исправлено при генерации
        return 'ERROR_NEEDS_BIRTH_CHECK'


def generate_error_gender():
    """Генерирует пол с возможными ошибками"""
    error_types = [
        'normal'  # М или Ж
    ]

    error = 'normal'

    if error == 'normal':
        return random.choice(['М', 'Ж'])


def generate_name_and_birth_check(birth_date, hire_date):
    """Проверяет и корректирует даты для логических ошибок"""
    if hire_date == 'ERROR_NEEDS_BIRTH_CHECK' and birth_date != 'ERROR':
        try:
            birth = date.fromisoformat(birth_date)
            # Прием раньше рождения
            hire_date = (birth - timedelta(days=random.randint(1, 3650))).isoformat()
        except:
            hire_date = fake.date_between(start_date='-10y', end_date='today').isoformat()
    return hire_date


# Генерация 100 записей
records = []
for i in range(100):
    birth_date = generate_error_age()
    hire_date = generate_error_hire_date()
    salary = generate_error_salary()
    series, number = generate_error_passport()

    # Логическая ошибка: дата приема раньше рождения
    if hire_date == 'ERROR_NEEDS_BIRTH_CHECK':
        hire_date = generate_name_and_birth_check(birth_date, hire_date)

    record = {
        'full_name': fake.name(),
        'birth_date': birth_date,
        'hire_date': hire_date,
        'position': random.choice(POSITIONS),
        'salary': salary,
        'passport_series': series if series is not None else '',
        'passport_number': number if number is not None else '',
        'gender': generate_error_gender()
    }
    records.append(record)

# Создание DataFrame и сохранение
df = pd.DataFrame(records)
df.to_excel('test_mixed.xlsx', index=False)

# Статистика по сгенерированным данным
print("✅ test_mixed.xlsx создан (100 записей)")
print("\n📊 Статистика ошибок:")


# Анализ ошибок возраста
def check_age_error(birth_date):
    try:
        birth = date.fromisoformat(birth_date)
        age = (date.today() - birth).days // 365
        if birth > date.today():
            return "future"
        elif age > 150:
            return "too_old"
        elif age < 18:
            return "child"
        else:
            return "normal"
    except:
        return "invalid"


# Анализ ошибок зарплаты
def check_salary_error(salary):
    if isinstance(salary, str):
        return "string"
    elif salary < 0:
        return "negative"
    elif salary == 0:
        return "zero"
    elif salary > 1_000_000:
        return "huge"
    else:
        return "normal"


# Анализ ошибок паспорта
def check_passport_error(series, number):
    if pd.isna(series) or pd.isna(number) or series == '' or number == '':
        return "missing"
    series_str, number_str = str(series), str(number)
    if not series_str.isdigit() or not number_str.isdigit():
        return "letters"
    if len(series_str) != 4:
        return "invalid_series_len"
    if len(number_str) != 6:
        return "invalid_number_len"
    return "normal"


# Анализ ошибок даты приема
def check_hire_error(hire_date, birth_date):
    try:
        hire = date.fromisoformat(hire_date) if isinstance(hire_date, str) else hire_date
        if isinstance(hire, date):
            if hire > date.today():
                return "future"
            try:
                birth = date.fromisoformat(birth_date)
                if hire < birth:
                    return "before_birth"
            except:
                pass
            return "normal"
    except:
        return "invalid_format"
    return "normal"


# Сбор статистики
age_stats = {'normal': 0, 'future': 0, 'too_old': 0, 'child': 0, 'invalid': 0}
salary_stats = {'normal': 0, 'negative': 0, 'zero': 0, 'huge': 0, 'string': 0}
passport_stats = {'normal': 0, 'invalid_series_len': 0, 'invalid_number_len': 0, 'letters': 0, 'missing': 0}
hire_stats = {'normal': 0, 'future': 0, 'before_birth': 0, 'invalid_format': 0}
gender_stats = {'normal': 0, 'invalid': 0, 'empty': 0}

for _, row in df.iterrows():
    # Возраст
    age_error = check_age_error(row['birth_date'])
    age_stats[age_error] = age_stats.get(age_error, 0) + 1

    # Зарплата
    salary_error = check_salary_error(row['salary'])
    salary_stats[salary_error] = salary_stats.get(salary_error, 0) + 1

    # Паспорт
    passport_error = check_passport_error(row['passport_series'], row['passport_number'])
    passport_stats[passport_error] = passport_stats.get(passport_error, 0) + 1

    # Дата приема
    hire_error = check_hire_error(row['hire_date'], row['birth_date'])
    hire_stats[hire_error] = hire_stats.get(hire_error, 0) + 1

    # Пол
    if row['gender'] in ['М', 'Ж']:
        gender_stats['normal'] += 1
    elif row['gender'] == '':
        gender_stats['empty'] += 1
    else:
        gender_stats['invalid'] += 1

print(f"\n📅 Дата рождения: {age_stats}")
print(f"💰 Зарплата: {salary_stats}")
print(f"🛂 Паспорт: {passport_stats}")
print(f"📆 Дата приема: {hire_stats}")
print(f"⚥ Пол: {gender_stats}")

# Показываем несколько примеров ошибок
print("\n🔍 Примеры записей с ошибками:")
error_examples = df[
    (df['salary'].apply(lambda x: check_salary_error(x) != 'normal')) |
    (df['gender'].apply(lambda x: x not in ['М', 'Ж'])) |
    (df['birth_date'].apply(lambda x: check_age_error(x) != 'normal'))
    ].head(10)

for idx, row in error_examples.iterrows():
    print(f"\n{idx + 1}. {row['full_name']}")
    print(f"   Рождение: {row['birth_date']}, Прием: {row['hire_date']}")
    print(f"   Зарплата: {row['salary']}, Пол: '{row['gender']}'")
    print(f"   Паспорт: {row['passport_series']} {row['passport_number']}")