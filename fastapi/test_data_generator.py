"""
Генератор тестовых данных для экспериментальной оценки сервиса Data Quality.
Формирует Excel-файл с контролируемым уровнем зашумления и JSON-манифест
с разметкой каждой строки для расчёта Precision/Recall.

Использование:
    python test_data_generator.py --size 1000 --noise 0.15 --duplicates 0.02 \
        --seed 42 --output datasets/DS-1000-mid.xlsx
"""
import argparse
import json
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from faker import Faker

POSITIONS_VALID = [
    "инженер", "бухгалтер", "менеджер", "директор", "программист",
    "аналитик", "водитель", "разработчик", "тестировщик", "юрист",
]
POSITIONS_INVALID = ["главный инженер", "senior developer", "неопределено", ""]


# ============================================================
# Функции вычисления контрольных сумм (для валидных ИНН/СНИЛС)
# ============================================================

def _valid_inn_12() -> str:
    """Генерация валидного 12-значного ИНН с контрольными цифрами."""
    base = [random.randint(0, 9) for _ in range(10)]
    coeffs_1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
    coeffs_2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
    n11 = sum(base[i] * coeffs_1[i] for i in range(10)) % 11 % 10
    full = base + [n11]
    n12 = sum(full[i] * coeffs_2[i] for i in range(11)) % 11 % 10
    return "".join(map(str, full + [n12]))


def _valid_snils() -> str:
    """Генерация валидного СНИЛС с контрольной суммой."""
    digits = [random.randint(0, 9) for _ in range(9)]
    total = sum(d * (9 - i) for i, d in enumerate(digits))
    check = total % 101
    if check == 100:
        check = 0
    return "".join(map(str, digits)) + f"{check:02d}"


# ============================================================
# Генераторы значений с управляемым зашумлением
# ============================================================

def gen_birth_date(fake: Faker, corrupt: bool) -> tuple[Any, str | None]:
    if not corrupt:
        return fake.date_of_birth(minimum_age=22, maximum_age=60).isoformat(), None
    kind = random.choice(["future", "too_old", "underage", "bad_format"])
    if kind == "future":
        return (date.today() + timedelta(days=random.randint(1, 365))).isoformat(), "birth_date_future"
    if kind == "too_old":
        return (date.today() - timedelta(days=365 * random.randint(101, 150))).isoformat(), "birth_date_too_old"
    if kind == "underage":
        return (date.today() - timedelta(days=365 * random.randint(5, 17))).isoformat(), "birth_date_underage"
    return f"{random.randint(1, 31)}/{random.randint(1, 12)}/not_a_year", "birth_date_bad_format"


def gen_hire_date(fake: Faker, birth_iso: str, corrupt: bool) -> tuple[Any, str | None]:
    if not corrupt:
        try:
            birth = date.fromisoformat(birth_iso)
            start = birth + timedelta(days=365 * 18)
            if start >= date.today():
                start = date.today() - timedelta(days=365)
            return fake.date_between(start_date=start, end_date="today").isoformat(), None
        except ValueError:
            return fake.date_between(start_date="-10y", end_date="today").isoformat(), None
    kind = random.choice(["future", "before_birth", "bad_format"])
    if kind == "future":
        return (date.today() + timedelta(days=random.randint(1, 365))).isoformat(), "hire_date_future"
    if kind == "before_birth":
        try:
            birth = date.fromisoformat(birth_iso)
            return (birth - timedelta(days=random.randint(1, 365))).isoformat(), "hire_date_before_birth"
        except ValueError:
            return "1900-01-01", "hire_date_before_birth"
    return f"{random.randint(1, 31)}.{random.randint(13, 20)}.2024", "hire_date_bad_format"


def gen_salary(corrupt: bool) -> tuple[Any, str | None]:
    if not corrupt:
        return random.randint(25_000, 300_000), None
    kind = random.choice(["negative", "zero", "below_mrot", "huge", "string", "missing"])
    if kind == "negative":
        return -random.randint(1_000, 100_000), "salary_negative"
    if kind == "zero":
        return 0, "salary_zero"
    if kind == "below_mrot":
        return random.randint(1_000, 19_999), "salary_below_mrot"
    if kind == "huge":
        return random.randint(5_000_001, 10_000_000), "salary_huge"
    if kind == "string":
        return f"{random.randint(10_000, 500_000)} руб.", "salary_string"
    return None, "salary_missing"


def gen_passport(corrupt: bool) -> tuple[str, str, str | None]:
    if not corrupt:
        return f"{random.randint(1000, 9999):04d}", f"{random.randint(100000, 999999):06d}", None
    kind = random.choice(["short_series", "long_series", "short_number", "letters", "missing"])
    series = f"{random.randint(1000, 9999):04d}"
    number = f"{random.randint(100000, 999999):06d}"
    if kind == "short_series":
        return f"{random.randint(0, 999):03d}", number, "passport_series_short"
    if kind == "long_series":
        return str(random.randint(10_000, 99_999)), number, "passport_series_long"
    if kind == "short_number":
        return series, f"{random.randint(0, 99_999):05d}", "passport_number_short"
    if kind == "letters":
        return series, f"AB{random.randint(1000, 9999)}", "passport_letters"
    return "", "", "passport_missing"


def gen_inn(corrupt: bool) -> tuple[Any, str | None]:
    if not corrupt:
        return _valid_inn_12(), None
    kind = random.choice(["wrong_length", "bad_checksum", "missing"])
    if kind == "wrong_length":
        return str(random.randint(10**7, 10**8)), "inn_wrong_length"
    if kind == "bad_checksum":
        return "".join(str(random.randint(0, 9)) for _ in range(12)), "inn_bad_checksum"
    return None, "inn_missing"


def gen_snils(corrupt: bool) -> tuple[Any, str | None]:
    if not corrupt:
        return _valid_snils(), None
    kind = random.choice(["bad_checksum", "missing"])
    if kind == "bad_checksum":
        return "".join(str(random.randint(0, 9)) for _ in range(11)), "snils_bad_checksum"
    return None, "snils_missing"


def gen_gender(corrupt: bool) -> tuple[str, str | None]:
    if not corrupt:
        return random.choice(["М", "Ж"]), None
    return random.choice(["M", "F", "Муж", "male", "?"]), "gender_invalid"


def gen_position(corrupt: bool) -> tuple[str, str | None]:
    if not corrupt:
        return random.choice(POSITIONS_VALID), None
    return random.choice(POSITIONS_INVALID), "position_not_in_dict"


# ============================================================
# Основной генератор
# ============================================================

def generate_dataset(
    size: int,
    noise: float,
    duplicates: float,
    seed: int | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)
    fake = Faker("ru_RU")

    records: list[dict] = []
    manifest: list[dict] = []

    for i in range(size):
        is_corrupt = random.random() < noise
        # Если запись портим — вносим 1–3 дефекта в разные поля
        fields_to_corrupt = set()
        if is_corrupt:
            k = random.randint(1, 3)
            fields_to_corrupt = set(random.sample(
                ["birth_date", "hire_date", "salary", "passport",
                 "inn", "snils", "gender", "position"],
                k=k,
            ))

        defects: list[str] = []

        birth, d = gen_birth_date(fake, "birth_date" in fields_to_corrupt)
        if d: defects.append(d)
        hire, d = gen_hire_date(fake, str(birth), "hire_date" in fields_to_corrupt)
        if d: defects.append(d)
        salary, d = gen_salary("salary" in fields_to_corrupt)
        if d: defects.append(d)
        series, number, d = gen_passport("passport" in fields_to_corrupt)
        if d: defects.append(d)
        inn, d = gen_inn("inn" in fields_to_corrupt)
        if d: defects.append(d)
        snils, d = gen_snils("snils" in fields_to_corrupt)
        if d: defects.append(d)
        gender, d = gen_gender("gender" in fields_to_corrupt)
        if d: defects.append(d)
        position, d = gen_position("position" in fields_to_corrupt)
        if d: defects.append(d)

        record = {
            "last_name": fake.last_name(),
            "first_name": fake.first_name(),
            "middle_name": fake.middle_name() if random.random() > 0.1 else None,
            "birth_date": birth,
            "hire_date": hire,
            "termination_date": None,
            "position": position,
            "salary": salary,
            "passport_series": series,
            "passport_number": number,
            "gender": gender,
            "inn": inn,
            "snils": snils,
            "phone": fake.phone_number(),
            "email": fake.email(),
        }
        records.append(record)
        manifest.append({
            "row": i + 2,  # +2 из-за заголовка Excel и 1-индексации
            "status": "corrupt" if defects else "valid",
            "defects": defects,
        })

    # Внесение дубликатов
    dup_count = int(size * duplicates)
    for _ in range(dup_count):
        src_idx = random.randrange(size)
        records.append(records[src_idx].copy())
        manifest.append({
            "row": len(records) + 1,
            "status": "duplicate",
            "defects": ["duplicate_record"],
            "source_row": src_idx + 2,
        })

    df = pd.DataFrame(records)

    string_columns = ["inn", "snils", "passport_series", "passport_number", "phone"]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({"nan": None, "None": None})

    return df, manifest


def main():
    parser = argparse.ArgumentParser(description="Генератор тестовых данных для DQ-сервиса")
    parser.add_argument("--size", type=int, default=1000, help="Число корректных/зашумлённых записей")
    parser.add_argument("--noise", type=float, default=0.0, help="Доля зашумлённых записей (0..1)")
    parser.add_argument("--duplicates", type=float, default=0.0, help="Доля дубликатов (0..1)")
    parser.add_argument("--seed", type=int, default=None, help="Зерно случайности")
    parser.add_argument("--output", type=str, default="dataset.xlsx", help="Путь для Excel-файла")
    args = parser.parse_args()

    df, manifest = generate_dataset(args.size, args.noise, args.duplicates, args.seed)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_path, index=False)
    with out_path.with_suffix(".manifest.json").open("w", encoding="utf-8") as f:
        json.dump({
            "parameters": vars(args),
            "stats": {
                "total": len(df),
                "valid": sum(1 for m in manifest if m["status"] == "valid"),
                "corrupt": sum(1 for m in manifest if m["status"] == "corrupt"),
                "duplicates": sum(1 for m in manifest if m["status"] == "duplicate"),
            },
            "manifest": manifest,
        }, f, ensure_ascii=False, indent=2)

    print(f" {out_path} ({len(df)} записей)")
    print(f" valid:      {sum(1 for m in manifest if m['status'] == 'valid')}")
    print(f" corrupt:    {sum(1 for m in manifest if m['status'] == 'corrupt')}")
    print(f" duplicates: {sum(1 for m in manifest if m['status'] == 'duplicate')}")


if __name__ == "__main__":
    main()
