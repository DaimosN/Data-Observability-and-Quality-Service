"""
Скрипт оценки корректности валидации (Эксперимент Э1).
Сравнивает ground truth из manifest.json с результатами API (JSON-ответ).

Использование:
    python evaluate_e1.py --manifest datasets/DS-1000-mid.xlsx.manifest.json \
                          --results results_DS-1000-mid.json \
                          --output eval_DS-1000-mid.json
"""
import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict
import re


def load_manifest(path: str) -> Tuple[List[dict], dict]:
    """Загружает manifest.json с разметкой строк."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["manifest"], data["stats"]


def load_api_results(path: str) -> dict:
    """Загружает JSON-ответ API."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def map_manifest_status(manifest_item: dict) -> str:
    """
    Приводит статус из манифеста к бинарному:
      - 'corrupt' или 'duplicate' -> 'quarantine' (дефектная запись)
      - 'valid' -> 'approved' (корректная запись)
    """
    if manifest_item["status"] in ("corrupt", "duplicate"):
        return "quarantine"
    return "approved"


def map_api_status(api_result: dict, row_number: int) -> str:
    """
    Определяет статус строки по ответу API.
    Ищет строку в списке errors_detail; если есть — 'quarantine', иначе 'approved'.
    """
    for err_detail in api_result.get("errors_detail", []):
        if err_detail["row"] == row_number:
            return "quarantine"
    return "approved"


def get_predicted_defect_types(api_result: dict, row_number: int) -> List[str]:
    """
    Извлекает типы ошибок, которые система нашла в конкретной строке.
    Сопоставляет текст ошибки с кодами из манифеста.
    Возвращает список вида ['birth_date_future', 'salary_below_mrot', ...]
    """
    defects: List[str] = []
    for err_detail in api_result.get("errors_detail", []):
        if err_detail["row"] == row_number:
            for err in err_detail["errors"]:
                field = err.get("field", "")
                message = err.get("message", "")
                matched = match_error_to_defect(field, message)
                if matched:
                    defects.append(matched)
    return defects


def match_error_to_defect(field: str, message: str) -> str | None:
    """
    Сопоставляет сообщение об ошибке из API с кодом дефекта из манифеста.
    Анализирует реальные сообщения из твоего JSON.
    """
    msg = message.lower()

    # ============================================================
    # birth_date
    # ============================================================
    if field == "birth_date" or (field == "" and "birth_date" in msg):
        if "будущем" in msg:
            return "birth_date_future"
        if re.search(r"сотруднику\s+(\d+)\s+лет.*минимум\s+18", msg):
            return "birth_date_underage"
        if re.search(r"сотруднику\s+(\d+)\s+лет.*максимум\s+100", msg):
            return "birth_date_too_old"
        if "не удалось распознать дату" in msg or "not_a_year" in msg:
            return "birth_date_bad_format"

    # ============================================================
    # hire_date
    # ============================================================
    if field == "hire_date" or (field == "" and "hire_date" in msg):
        if "будущем" in msg or "future" in msg:
            return "hire_date_future"
        if "раньше даты рождения" in msg:
            return "hire_date_before_birth"
        if "не удалось распознать дату" in msg:
            return "hire_date_bad_format"

    # ============================================================
    # termination_date (на всякий случай)
    # ============================================================
    if field == "termination_date":
        if "будущем" in msg:
            return "termination_date_future"
        if "раньше даты приёма" in msg:
            return "termination_date_before_hire"

    # ============================================================
    # salary
    # ============================================================
    if field == "salary":
        if "greater than 0" in msg or "больше 0" in msg:
            # Может быть и нулевая, и отрицательная — проверяем точнее
            if "ниже мрот" in msg or "мрот" in msg:
                return "salary_below_mrot"
            if "negative" in msg or "отрицатель" in msg:
                return "salary_negative"
            return "salary_zero"
        if "ниже мрот" in msg or "мрот" in msg or "20" in msg:
            return "salary_below_mrot"
        if "превышает" in msg or "лимит" in msg or "5000000" in msg:
            return "salary_huge"
        if "string" in msg or "строка" in msg or "unable to parse string" in msg:
            return "salary_string"
        if "valid number" in msg:
            return "salary_string"  # невалидное число = строковое значение
        if "greater than 0" in msg:
            return "salary_zero"

    # ============================================================
    # passport
    # ============================================================
    if field == "passport_series":
        if "4 цифры" in msg:
            # Может быть короткая или длинная — проверяем через манифест
            return "passport_series_short"  # по умолчанию, т.к. точнее не определить
    if field == "passport_number":
        if "6 цифр" in msg:
            return "passport_number_short"
    # Общая ошибка паспорта
    if field == "" and "passport" in msg:
        if "заполнены" in msg:
            return "passport_missing"

    # ============================================================
    # gender
    # ============================================================
    if field == "gender":
        if "м" in msg or "ж" in msg:
            return "gender_invalid"

    # ============================================================
    # inn
    # ============================================================
    if field == "inn":
        if "контрольная сумма" in msg:
            return "inn_bad_checksum"
        if "формат" in msg:
            return "inn_bad_checksum"
        if "длина" in msg:
            return "inn_wrong_length"

    # ============================================================
    # snils
    # ============================================================
    if field == "snils":
        if "контрольная сумма" in msg:
            return "snils_bad_checksum"
        if "формат" in msg:
            return "snils_bad_checksum"

    # ============================================================
    # position
    # ============================================================
    if field == "position":
        if "не найдена в справочнике" in msg:
            return "position_not_in_dict"
        if "valid string" in msg:
            return "position_not_in_dict"  # пустая строка или невалидный тип

    # ============================================================
    # phone
    # ============================================================
    if field == "phone":
        if "формат" in msg or "неверный" in msg:
            return "phone_invalid"

    # ============================================================
    # email
    # ============================================================
    if field == "email":
        if "формат" in msg or "неверный" in msg:
            return "email_invalid"

    # ============================================================
    # name fields (last_name, first_name, middle_name)
    # ============================================================
    if field in ("last_name", "first_name", "middle_name"):
        if "буквы" in msg or "дефис" in msg or "символа" in msg:
            return f"{field}_invalid_format"

    # Если ничего не подошло, возвращаем None
    return None


def compute_overall_metrics(
    manifest: List[dict], api_result: dict
) -> Dict[str, float]:
    """
    Считает общие Precision, Recall, F1 для бинарной классификации
    (approved vs quarantine).
    """
    tp = 0  # дефектная и отправлена в карантин
    fp = 0  # корректная, но отправлена в карантин
    fn = 0  # дефектная, но пропущена как корректная
    tn = 0  # корректная и пропущена

    for item in manifest:
        row_num = item["row"]
        ground = map_manifest_status(item)
        predicted = map_api_status(api_result, row_num)

        if ground == "quarantine" and predicted == "quarantine":
            tp += 1
        elif ground == "approved" and predicted == "quarantine":
            fp += 1
        elif ground == "quarantine" and predicted == "approved":
            fn += 1
        else:  # ground == "approved" and predicted == "approved"
            tn += 1

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    return {
        "total_records": len(manifest),
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "true_negative": tn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
    }


def compute_per_defect_metrics(
    manifest: List[dict], api_result: dict
) -> Dict[str, dict]:
    """
    Считает Precision / Recall по каждому типу дефекта отдельно.
    Возвращает словарь: {defect_type: {tp, fp, fn, precision, recall}}
    """
    counters: Dict[str, dict] = defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0})

    for item in manifest:
        row_num = item["row"]

        # --- Ground truth: какие дефекты реально есть ---
        true_defects = set()
        for d in item.get("defects", []):
            true_defects.add(d)

        # --- Predicted: какие дефекты нашла система ---
        predicted_defects = set(get_predicted_defect_types(api_result, row_num))

        # Собираем все уникальные типы дефектов
        all_defect_types = true_defects | predicted_defects

        for defect_type in all_defect_types:
            in_ground = defect_type in true_defects
            in_predicted = defect_type in predicted_defects

            if in_ground and in_predicted:
                counters[defect_type]["tp"] += 1
            elif in_ground and not in_predicted:
                counters[defect_type]["fn"] += 1
            elif not in_ground and in_predicted:
                counters[defect_type]["fp"] += 1

    # Считаем Precision и Recall для каждого типа
    result = {}
    for defect_type, c in counters.items():
        tp = c["tp"]
        fp = c["fp"]
        fn = c["fn"]
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        result[defect_type] = {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
        }
    return result


def main():
    parser = argparse.ArgumentParser(description="Оценка корректности валидации (Э1)")
    parser.add_argument("--manifest", type=str, required=True, help="Путь к .manifest.json")
    parser.add_argument("--results", type=str, required=True, help="Путь к JSON-ответу API")
    parser.add_argument("--output", type=str, default=None, help="Куда сохранить итоговый JSON")
    args = parser.parse_args()

    # Загрузка данных
    manifest, stats = load_manifest(args.manifest)
    api_result = load_api_results(args.results)

    # Сверка общего количества записей
    manifest_total = len(manifest)
    api_total = api_result.get("total", 0)
    print(f"[INFO] Манифест: {manifest_total} записей, API: {api_total} записей")

    if manifest_total != api_total:
        print("[WARN] Количество записей не совпадает! Проверь, что манифест соответствует файлу.")

    # Расчёт метрик
    overall = compute_overall_metrics(manifest, api_result)
    per_defect = compute_per_defect_metrics(manifest, api_result)

    # --- Вывод в консоль ---
    print("\n" + "=" * 70)
    print("  ОБЩИЕ МЕТРИКИ КОРРЕКТНОСТИ ВАЛИДАЦИИ")
    print(f"  Всего записей:        {overall['total_records']}")
    print(f"  True Positive (TP):   {overall['true_positive']}")
    print(f"  False Positive (FP):  {overall['false_positive']}")
    print(f"  False Negative (FN):  {overall['false_negative']}")
    print(f"  True Negative (TN):   {overall['true_negative']}")
    print("-" * 70)
    print(f"  Precision: {overall['precision']:.4f}")
    print(f"  Recall:    {overall['recall']:.4f}")
    print(f"  F1-score:  {overall['f1_score']:.4f}")

    print("  МЕТРИКИ ПО ТИПАМ ДЕФЕКТОВ")
    print(f"  {'Тип дефекта':<30s} {'Precision':>10s} {'Recall':>10s} {'TP':>6s} {'FP':>6s} {'FN':>6s}")
    print("-" * 70)
    for defect_type in sorted(per_defect.keys()):
        m = per_defect[defect_type]
        print(f"  {defect_type:<30s} {m['precision']:>10.4f} {m['recall']:>10.4f} {m['tp']:>6d} {m['fp']:>6d} {m['fn']:>6d}")

    # Сохранение в JSON
    result = {
        "dataset": Path(args.manifest).stem.replace(".xlsx.manifest", ""),
        "manifest_stats": stats,
        "api_stats": {
            "total": api_result.get("total", 0),
            "approved": api_result.get("approved", 0),
            "quarantine": api_result.get("quarantine", 0),
        },
        "overall_metrics": overall,
        "per_defect_metrics": per_defect,
    }

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\n[INFO] Результаты сохранены в: {args.output}")
    else:
        out_name = f"eval_{Path(args.manifest).stem.replace('.xlsx.manifest', '')}.json"
        with open(out_name, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\n[INFO] Результаты сохранены в: {out_name}")


if __name__ == "__main__":
    main()