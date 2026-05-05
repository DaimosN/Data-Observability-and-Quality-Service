"""
Модуль метрик для мониторинга качества данных.
Использует prometheus_client для сбора и экспорта метрик.
"""
import time
import logging
from datetime import datetime

from prometheus_client import Counter, Gauge, Histogram, Info

logger = logging.getLogger(__name__)

# ============================================================
# 1. Метрики валидации (Counter)
# ============================================================

dq_validations_total = Counter(
    "dq_validations_total",
    "Total number of data validations performed",
    labelnames=["status", "source"],
)

dq_validation_errors = Counter(
    "dq_validation_errors_total",
    "Total validation errors by type",
    labelnames=["error_type", "field"],
)

dq_files_processed = Counter(
    "dq_files_processed_total",
    "Total files processed by the service",
    labelnames=["file_type", "status"],
)

# ============================================================
# 2. Метрики состояния (Gauge)
# ============================================================

dq_quarantine_size = Gauge(
    "dq_quarantine_size",
    "Current number of records in quarantine",
    labelnames=["status"],
)

dq_overall_score = Gauge(
    "dq_overall_score",
    "Overall data quality score (0-100)",
    labelnames=["table_name"],
)

dq_completeness = Gauge(
    "dq_completeness",
    "Data completeness percentage per column",
    labelnames=["table_name", "column_name"],
)

dq_uniqueness = Gauge(
    "dq_uniqueness",
    "Data uniqueness percentage",
    labelnames=["table_name", "column_name"],
)

dq_freshness_hours = Gauge(
    "dq_freshness_hours",
    "Hours since last data update",
    labelnames=["table_name"],
)

dq_anomaly_count = Gauge(
    "dq_anomaly_count",
    "Number of detected anomalies",
    labelnames=["table_name", "column_name", "anomaly_type"],
)

dq_table_size = Gauge(
    "dq_table_size_rows",
    "Total number of rows in the table",
    labelnames=["table_name"],
)

dq_duplicate_groups = Gauge(
    "dq_duplicate_groups",
    "Number of duplicate groups found",
    labelnames=["table_name"],
)

dq_last_load_timestamp = Gauge(
    "dq_last_load_timestamp_seconds",
    "Timestamp of the last successful data load",
    labelnames=["table_name"],
)

dq_salary_distribution = Gauge(
    "dq_salary_distribution",
    "Distribution of salaries by range",
    labelnames=["salary_range"],
)

dq_active_rules = Gauge(
    "dq_active_rules",
    "Number of active validation rules",
    labelnames=["rule_category"],
)

# ============================================================
# 3. Метрики производительности (Histogram)
# ============================================================

dq_validation_duration = Histogram(
    "dq_validation_duration_seconds",
    "Time spent on data validation",
    labelnames=["validation_type"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)

dq_batch_size = Histogram(
    "dq_batch_size_records",
    "Number of records per batch",
    labelnames=["source"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000],
)

# ============================================================
# 4. Информационные метрики
# ============================================================

dq_service_info = Info("dq_service", "Data Quality Service information")


# ============================================================
# 5. Вспомогательные функции
# ============================================================

def init_metrics(service_version: str = "1.0.0", environment: str = "production"):
    """Инициализация информационных метрик при старте сервиса."""
    dq_service_info.info({
        "version": service_version,
        "environment": environment,
        "python_version": "3.11",
    })

    # Инициализация счётчиков активных правил
    rules = {"format": 10, "business": 5, "reference": 2}
    for category, count in rules.items():
        dq_active_rules.labels(rule_category=category).set(count)

    logger.info(f"Metrics initialized: version={service_version}, env={environment}")


def record_validation_result(status: str, source: str = "api"):
    dq_validations_total.labels(status=status, source=source).inc()


def record_validation_error(error_type: str, field: str):
    dq_validation_errors.labels(error_type=error_type, field=field).inc()


def record_file_processed(file_type: str, success: bool):
    status = "success" if success else "failed"
    dq_files_processed.labels(file_type=file_type, status=status).inc()


def record_batch_size(source: str, size: int):
    dq_batch_size.labels(source=source).observe(size)


def update_completeness_metric(table_name: str, column_name: str, completeness_pct: float):
    dq_completeness.labels(table_name=table_name, column_name=column_name).set(completeness_pct)


def update_overall_score(table_name: str, score: float):
    dq_overall_score.labels(table_name=table_name).set(score)


def update_uniqueness_metric(table_name: str, column_name: str, uniqueness_pct: float):
    dq_uniqueness.labels(table_name=table_name, column_name=column_name).set(uniqueness_pct)


def update_freshness_metric(table_name: str, hours: float):
    dq_freshness_hours.labels(table_name=table_name).set(hours)


def update_anomaly_count(table_name: str, column_name: str, anomaly_type: str, count: int):
    dq_anomaly_count.labels(
        table_name=table_name, column_name=column_name, anomaly_type=anomaly_type
    ).set(count)


def update_table_size_metric(table_name: str, row_count: int):
    dq_table_size.labels(table_name=table_name).set(row_count)


def update_last_load_timestamp(table_name: str):
    dq_last_load_timestamp.labels(table_name=table_name).set(datetime.now().timestamp())


def update_duplicate_metric(table_name: str, duplicate_count: int):
    dq_duplicate_groups.labels(table_name=table_name).set(duplicate_count)


def update_salary_distribution(salary_range: str, count: int):
    dq_salary_distribution.labels(salary_range=salary_range).set(count)


def update_quarantine_size(status: str, count: int):
    dq_quarantine_size.labels(status=status).set(count)


# ============================================================
# 6. Контекстный менеджер для замера времени
# ============================================================

class ValidationTimer:
    """
    Контекстный менеджер для замера времени валидации.
        with ValidationTimer("batch"):
            ...
    """

    def __init__(self, validation_type: str):
        self.validation_type = validation_type
        self._histogram = dq_validation_duration.labels(validation_type=validation_type)

    def __enter__(self):
        self._start = time.time()
        return self

    def __exit__(self, *args):
        duration = time.time() - self._start
        self._histogram.observe(duration)
        logger.debug(f"ValidationTimer[{self.validation_type}]: {duration:.3f}s")
