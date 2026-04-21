"""
Модуль метрик для мониторинга качества данных.
Использует prometheus_client для сбора и экспорта метрик.
"""
import time
from datetime import datetime

from prometheus_client import Counter, Gauge, Histogram, Info, REGISTRY
import logging

logger = logging.getLogger(__name__)

# ============================================================
# 1. Метрики валидации (счётчики событий)
# ============================================================

# Общее количество валидаций с разбивкой по статусу
dq_validations_total = Counter(
    'dq_validations_total',
    'Total number of data validations performed',
    labelnames=['status', 'source']
)

# Счётчик ошибок по типам (для анализа частоты проблем)
dq_validation_errors = Counter(
    'dq_validation_errors_total',
    'Total validation errors by type',
    labelnames=['error_type', 'field']  # error_type: age_invalid, position_missing и т.д.
)

# Счётчик обработанных файлов
dq_files_processed = Counter(
    'dq_files_processed_total',
    'Total files processed by the service',
    labelnames=['file_type', 'status']  # status: success, failed
)

# ============================================================
# 2. Метрики состояния (текущие значения)
# ============================================================

# Текущий размер карантина по статусам
dq_quarantine_size = Gauge(
    'dq_quarantine_size',
    'Current number of records in quarantine',
    labelnames=['status']  # status: new, in_review, fixed, rejected
)

# Общая оценка качества данных (0-100)
dq_overall_score = Gauge(
    'dq_overall_score',
    'Overall data quality score (0-100)',
    labelnames=['table_name']
)

# Полнота данных по колонкам (процент не-NULL значений)
dq_completeness = Gauge(
    'dq_completeness',
    'Data completeness percentage per column',
    labelnames=['table_name', 'column_name']
)

# Уникальность данных (процент уникальных значений)
dq_uniqueness = Gauge(
    'dq_uniqueness',
    'Data uniqueness percentage',
    labelnames=['table_name', 'column_name']
)

# Свежесть данных (часы с момента последнего обновления)
dq_freshness_hours = Gauge(
    'dq_freshness_hours',
    'Hours since last data update',
    labelnames=['table_name']
)

# Количество аномалий (статистических выбросов)
dq_anomaly_count = Gauge(
    'dq_anomaly_count',
    'Number of detected anomalies',
    labelnames=['table_name', 'column_name', 'anomaly_type']
)

dq_table_size = Gauge(
    'dq_table_size_rows',
    'Total number of rows in the table',
    labelnames=['table_name']
)

dq_duplicate_groups = Gauge(
    'dq_duplicate_groups',
    'Number of duplicate groups found',
    labelnames=['table_name']
)

dq_last_load_timestamp = Gauge(
    'dq_last_load_timestamp_seconds',
    'Timestamp of the last successful data load',
    labelnames=['table_name']
)

# ============================================================
# 3. Метрики производительности (гистограммы)
# ============================================================

# Время выполнения валидации
dq_validation_duration = Histogram(
    'dq_validation_duration_seconds',
    'Time spent on data validation',
    labelnames=['validation_type'],  # validation_type: single, batch, full_scan
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
)

# Размер обрабатываемых данных
dq_batch_size = Histogram(
    'dq_batch_size_records',
    'Number of records per batch',
    labelnames=['source'],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000]
)

dq_salary_distribution = Gauge(
    'dq_salary_distribution',
    'Distribution of salaries by range',
    labelnames=['salary_range']
)

# ============================================================
# 4. Информационные метрики (версия, конфигурация)
# ============================================================

# Информация о сервисе (версия, окружение)
dq_service_info = Info(
    'dq_service',
    'Data Quality Service information'
)

# Активные правила валидации (список)
dq_active_rules = Gauge(
    'dq_active_rules',
    'Number of active validation rules',
    labelnames=['rule_category']  # rule_category: format, business, reference
)


# ============================================================
# 5. Вспомогательные функции для работы с метриками
# ============================================================

def init_metrics(service_version: str = "1.0.0", environment: str = "production"):
    """Инициализация информационных метрик при старте сервиса."""
    dq_service_info.info({
        'version': service_version,
        'environment': environment,
        'python_version': '3.11'
    })
    logger.info(f"Metrics initialized: version={service_version}, env={environment}")


def record_validation_result(status: str, source: str = "api"):
    """Утилитарная функция для записи результата валидации."""
    dq_validations_total.labels(status=status, source=source).inc()
    logger.debug(f"Validation result recorded: {status} from {source}")


def record_validation_error(error_type: str, field: str):
    """Запись конкретной ошибки валидации."""
    dq_validation_errors.labels(error_type=error_type, field=field).inc()


def record_file_processed(file_type: str, success: bool):
    """Запись факта обработки файла."""
    status = "success" if success else "failed"
    dq_files_processed.labels(file_type=file_type, status=status).inc()


def update_quarantine_metrics(db_connection):
    """Обновление метрик размера карантина."""
    try:
        cursor = db_connection.cursor()

        for status in ['new', 'in_review', 'fixed', 'rejected']:
            cursor.execute(
                "SELECT COUNT(*) FROM data_quality.quarantine_log WHERE dq_status = %s",
                (status,)
            )
            count = cursor.fetchone()[0]
            dq_quarantine_size.labels(status=status).set(count)

        cursor.close()
        logger.debug("Quarantine metrics updated")

    except Exception as e:
        logger.error(f"Failed to update quarantine metrics: {e}")


def update_completeness_metric(table_name: str, column_name: str, completeness_percent: float):
    """Обновление метрики полноты данных."""
    dq_completeness.labels(table_name=table_name, column_name=column_name).set(completeness_percent)


def update_overall_score(table_name: str, score: float):
    """Обновление общей оценки качества данных."""
    dq_overall_score.labels(table_name=table_name).set(score)


def update_uniqueness_metric(table_name: str, column_name: str, uniqueness_percent: float):
    """Обновление метрики уникальности данных."""
    dq_uniqueness.labels(table_name=table_name, column_name=column_name).set(uniqueness_percent)


def update_freshness_metric(table_name: str, hours: float):
    """Обновление метрики свежести данных."""
    dq_freshness_hours.labels(table_name=table_name).set(hours)


def update_anomaly_count(table_name: str, column_name: str, anomaly_type: str, count: int):
    """Обновление счетчика аномалий."""
    dq_anomaly_count.labels(
        table_name=table_name,
        column_name=column_name,
        anomaly_type=anomaly_type
    ).set(count)


def update_active_rules(rule_category: str, count: int):
    """Обновление количества активных правил."""
    dq_active_rules.labels(rule_category=rule_category).set(count)


def record_batch_size(source: str, size: int):
    """Запись размера пакета в гистограмму."""
    dq_batch_size.labels(source=source).observe(size)


def update_table_size_metric(table_name: str, row_count: int):
    """Обновление метрики размера таблицы."""
    dq_table_size.labels(table_name=table_name).set(row_count)


def update_last_load_timestamp(table_name: str):
    """Обновление метрики времени последней загрузки."""
    dq_last_load_timestamp.labels(table_name=table_name).set(datetime.now().timestamp())


def update_duplicate_metric(table_name: str, duplicate_count: int):
    """Обновление метрики количества дубликатов."""
    dq_duplicate_groups.labels(table_name=table_name).set(duplicate_count)


def update_salary_distribution(salary_range: str, count: int):
    """Обновление распределения зарплат."""
    dq_salary_distribution.labels(salary_range=salary_range).set(count)


# ============================================================
# 6. Класс-обёртка для контекстного менеджмента
# ============================================================

class ValidationTimer:
    """
    Контекстный менеджер для замера времени валидации.
    Использование:
        with ValidationTimer("batch"):
            # валидация...
    """

    def __init__(self, validation_type: str):
        self.validation_type = validation_type
        self._histogram = dq_validation_duration.labels(validation_type=self.validation_type)

    def __enter__(self):
        self._start = time.time()
        return self

    def __exit__(self, *args):
        duration = time.time() - self._start
        self._histogram.observe(duration)
        logger.debug(f"Validation timer stopped for type: {self.validation_type}, duration: {duration:.3f}s")


# ============================================================
# 7. Инициализация при старте (если нужно зарегистрировать кастомные метрики)
# ============================================================

def register_custom_metrics():
    """Регистрация всех метрик."""
    logger.info(f"Registered metrics: {[metric.name for metric in REGISTRY.collect()]}")
