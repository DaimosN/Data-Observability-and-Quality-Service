from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'dq_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}


def check_completeness():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    query = """
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN full_name IS NULL THEN 1 ELSE 0 END) as null_full_name,
            SUM(CASE WHEN birth_date IS NULL THEN 1 ELSE 0 END) as null_birth_date
        FROM hr.employees
    """
    result = pg_hook.get_first(query)
    completeness = 100 - ((result[1] + result[2]) / (result[0] * 2)) * 100

    pg_hook.run("""
        INSERT INTO data_quality.dq_metrics_history (metric_name, table_name, metric_value, status)
        VALUES ('completeness', 'hr.employees', %s, 
                CASE WHEN %s >= 95 THEN 'OK' WHEN %s >= 90 THEN 'WARNING' ELSE 'CRITICAL' END)
    """, parameters=(completeness, completeness, completeness))


def check_anomalies():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    query = """
        SELECT COUNT(*) as anomalies FROM hr.employees
        WHERE salary < 0 OR salary > 1000000
    """
    anomalies = pg_hook.get_first(query)[0]

    pg_hook.run("""
        INSERT INTO data_quality.dq_metrics_history (metric_name, table_name, metric_value, status)
        VALUES ('anomalies', 'hr.employees.salary', %s,
                CASE WHEN %s = 0 THEN 'OK' WHEN %s <= 5 THEN 'WARNING' ELSE 'CRITICAL' END)
    """, parameters=(anomalies, anomalies, anomalies))


with DAG('data_quality_monitoring', schedule_interval='0 2 * * *', catchup=False, default_args=default_args) as dag:
    t1 = PythonOperator(task_id='check_completeness', python_callable=check_completeness)
    t2 = PythonOperator(task_id='check_anomalies', python_callable=check_anomalies)
    t1 >> t2
