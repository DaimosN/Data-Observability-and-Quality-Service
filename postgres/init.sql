CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS data_quality;

-- Production
CREATE TABLE hr.employees (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(200) NOT NULL,
    birth_date DATE NOT NULL,
    hire_date DATE NOT NULL,
    termination_date DATE,
    position VARCHAR(100),
    salary NUMERIC(10,2),
    passport_data VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Карантин
CREATE TABLE data_quality.quarantine_log (
    id SERIAL PRIMARY KEY,
    source_file VARCHAR(255),
    raw_data JSONB,
    validation_errors JSONB,
    dq_status VARCHAR(20) DEFAULT 'new',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- История метрик
CREATE TABLE data_quality.dq_metrics_history (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100),
    table_name VARCHAR(100),
    metric_value NUMERIC,
    status VARCHAR(20),
    check_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);