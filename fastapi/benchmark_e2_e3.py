"""
Бенчмарк производительности загрузки (Эксперименты Э2 и Э3).

Что измеряет:
  - Э2: throughput (rec/s) для файлов разного размера (INSERT + executemany + COPY)
  - Э3: сравнение трёх стратегий записи на трёх размерах файлов

Использование:
  python benchmark_e2_e3.py --all
  python benchmark_e2_e3.py --e2
  python benchmark_e2_e3.py --e3
"""
import argparse
import json
import time
import sys

import requests
import psycopg2

API_URL = "http://localhost:8000/upload/excel/"
DB_URL = "postgresql://dq_user:dq_pass@localhost:5432/dq_db"

DATASETS_E2 = {
    "100": "datasets/DS-100-clean.xlsx",
    "500": "datasets/DS-500-clean.xlsx",
    "1000": "datasets/DS-1000-clean.xlsx",
    "5000": "datasets/DS-5000-clean.xlsx",
    "10000": "datasets/DS-10000-clean.xlsx",
}

DATASETS_E3 = {
    "500": "datasets/DS-500-clean.xlsx",
    "1000": "datasets/DS-1000-clean.xlsx",
    "5000": "datasets/DS-5000-clean.xlsx",
}

REPEATS = 5  # Количество повторов для каждого замера


def clear_db():
    """Очищает таблицы hr.employees и data_quality.quarantine_log."""
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE hr.employees CASCADE;")
            cur.execute("TRUNCATE TABLE data_quality.quarantine_log CASCADE;")
        conn.commit()
    print("  [OK] Таблицы очищены")


def upload_file(filepath: str) -> tuple[float, dict]:
    """
    Загружает Excel-файл через API.
    Возвращает (время_в_секундах, ответ_json).
    """
    with open(filepath, "rb") as f:
        start = time.perf_counter()
        response = requests.post(API_URL, files={"file": f})
        elapsed = time.perf_counter() - start

    if response.status_code != 200:
        print(f"  [ERROR] HTTP {response.status_code}: {response.text[:200]}")
        return elapsed, {}
    return elapsed, response.json()


def run_experiment_e2():
    """
    Эксперимент Э2: Производительность загрузки.
    Загружаем файлы 5 раз, отбрасываем первый замер, считаем среднее и СКО.
    Вычисляем throughput (rec/s).
    """
    print("\n" + "=" * 60)
    print("  ЭКСПЕРИМЕНТ Э2: ПРОИЗВОДИТЕЛЬНОСТЬ ЗАГРУЗКИ")
    print("=" * 60)

    results = {}

    for label, filepath in DATASETS_E2.items():
        size = int(label)
        print(f"\n>>> Файл: {filepath} ({size} записей)")

        times = []
        for i in range(REPEATS + 1):
            clear_db()
            elapsed, resp = upload_file(filepath)
            times.append(elapsed)

            approved = resp.get("approved", "?")
            total = resp.get("total", "?")
            print(f"    Запуск {i + 1}/{REPEATS + 1}: {elapsed:.3f} с (approved={approved}, total={total})")

        # Отбрасываем первый (прогрев)
        warm = times[0]
        sample = times[1:]  # 5 замеров

        avg_time = sum(sample) / len(sample)
        # Среднеквадратичное отклонение
        variance = sum((t - avg_time) ** 2 for t in sample) / len(sample)
        stddev = variance ** 0.5
        throughput = size / avg_time

        results[label] = {
            "size": size,
            "warmup_time": round(warm, 3),
            "avg_time": round(avg_time, 3),
            "stddev": round(stddev, 3),
            "throughput": round(throughput, 1),
            "times": [round(t, 3) for t in times],
        }

        print(f"    >>> Среднее: {avg_time:.3f} с ± {stddev:.3f}, Throughput: {throughput:.1f} rec/s")

    # Сохранение
    out = "benchmark_e2.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n[SAVED] Результаты Э2 сохранены в {out}")

    # Итоговая таблица
    print("\n" + "-" * 60)
    print(f"  {'Размер':<10s} {'Время, с':<12s} {'СКО, с':<10s} {'Throughput, rec/s':<20s}")
    print("-" * 60)
    for label in ["100", "500", "1000", "5000", "10000"]:
        r = results[label]
        print(f"  {r['size']:<10d} {r['avg_time']:<12.3f} {r['stddev']:<10.3f} {r['throughput']:<20.1f}")


# ============================================================
# Подход для Э3 (без модификации кода FastAPI)
# ============================================================

def benchmark_sql_strategies():
    """
    Замеряет время выполнения INSERT, executemany, COPY напрямую в БД.
    """
    import io
    import pandas as pd

    print("\n" + "=" * 60)
    print("  Э3 (АЛЬТЕРНАТИВНЫЙ): ЗАМЕРЫ СТРАТЕГИЙ НА УРОВНЕ SQL")
    print("=" * 60)

    results = {}

    for label, filepath in DATASETS_E3.items():
        size = int(label)
        print(f"\n>>> Файл: {filepath} ({size} записей)")
        df = pd.read_excel(filepath)

        # Подготавливаем строки
        rows = []
        for _, row in df.iterrows():
            rows.append((
                row["last_name"],
                row["first_name"],
                row.get("middle_name") if pd.notna(row.get("middle_name")) else None,
                str(row["birth_date"])[:10],   # Берём только дату (YYYY-MM-DD)
                str(row["hire_date"])[:10],
                None,  # termination_date
                row["position"],
                float(row["salary"]),
                f"{row.get('passport_series','')} {row.get('passport_number','')}".strip() or None,
            ))

        strategies = {}

        # --- 1. INSERT по одной ---
        clear_db()
        times_insert = []
        for attempt in range(3):
            clear_db()
            with psycopg2.connect(DB_URL) as conn:
                with conn.cursor() as cur:
                    start = time.perf_counter()
                    for r in rows:
                        cur.execute(
                            """INSERT INTO hr.employees
                            (last_name, first_name, middle_name, birth_date, hire_date,
                             termination_date, position, salary, passport_data, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                            r,
                        )
                    conn.commit()
                    elapsed = time.perf_counter() - start
            times_insert.append(elapsed)
            print(f"    INSERT запуск {attempt+1}: {elapsed:.3f} с")
        strategies["INSERT"] = round(sum(times_insert) / len(times_insert), 3)

        # --- 2. executemany ---
        clear_db()
        times_em = []
        for attempt in range(3):
            clear_db()
            with psycopg2.connect(DB_URL) as conn:
                with conn.cursor() as cur:
                    start = time.perf_counter()
                    cur.executemany(
                        """INSERT INTO hr.employees
                        (last_name, first_name, middle_name, birth_date, hire_date,
                         termination_date, position, salary, passport_data, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                        rows,
                    )
                    conn.commit()
                    elapsed = time.perf_counter() - start
            times_em.append(elapsed)
            print(f"    executemany запуск {attempt+1}: {elapsed:.3f} с")
        strategies["executemany"] = round(sum(times_em) / len(times_em), 3)

        # --- 3. COPY ---
        clear_db()
        times_copy = []
        for attempt in range(3):
            clear_db()
            with psycopg2.connect(DB_URL) as conn:
                with conn.cursor() as cur:
                    start = time.perf_counter()
                    buf = io.StringIO()
                    for r in rows:
                        line_parts = []
                        for v in r:
                            if v is None:
                                line_parts.append("\\N")
                            else:
                                s = str(v).replace("\\", "\\\\").replace("\t", "\\t")
                                line_parts.append(s)
                        buf.write("\t".join(line_parts) + "\n")
                    buf.seek(0)

                    # Используем copy_from с указанием таблицы как строки
                    # и схемы через options
                    cur.execute('SET search_path TO hr, public;')
                    cur.copy_from(
                        buf,
                        "employees",
                        columns=(
                            "last_name", "first_name", "middle_name", "birth_date",
                            "hire_date", "termination_date", "position", "salary",
                            "passport_data",
                        ),
                        sep="\t",
                        null="\\N",
                    )
                    # Заполняем created_at для всех строк
                    cur.execute("UPDATE hr.employees SET created_at = NOW() WHERE created_at IS NULL;")
                    cur.execute('SET search_path TO public;')
                    conn.commit()
                    elapsed = time.perf_counter() - start
            times_copy.append(elapsed)
            print(f"    COPY запуск {attempt+1}: {elapsed:.3f} с")
        strategies["COPY"] = round(sum(times_copy) / len(times_copy), 3)

        results[label] = {
            "size": size,
            "strategies": strategies,
        }

        print(f"    >>> INSERT:       {strategies['INSERT']:.3f} с")
        print(f"    >>> executemany:  {strategies['executemany']:.3f} с")
        print(f"    >>> COPY:         {strategies['COPY']:.3f} с")
        if strategies["INSERT"] > 0:
            speedup = strategies["INSERT"] / strategies["COPY"]
            print(f"    >>> Ускорение COPY vs INSERT: ×{speedup:.1f}")

    # Сохранение
    out = "benchmark_e3.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n[SAVED] Результаты Э3 сохранены в {out}")


def main():
    parser = argparse.ArgumentParser(description="Бенчмарк Э2 и Э3")
    parser.add_argument("--all", action="store_true", help="Запустить оба эксперимента")
    parser.add_argument("--e2", action="store_true", help="Только Э2")
    parser.add_argument("--e3", action="store_true", help="Только Э3 (SQL-замеры)")
    args = parser.parse_args()

    if args.e2 or args.all:
        run_experiment_e2()
    if args.e3 or args.all:
        benchmark_sql_strategies()
    if not (args.e2 or args.e3 or args.all):
        print("Укажи --e2, --e3 или --all")
        sys.exit(1)


if __name__ == "__main__":
    main()
