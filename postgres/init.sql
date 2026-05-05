CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS data_quality;

-- ============================================================
-- Справочники (нужны до employees из-за FK)
-- ============================================================

CREATE TABLE IF NOT EXISTS hr.dict_positions (
    id              SERIAL PRIMARY KEY,
    position_name   VARCHAR(255) NOT NULL UNIQUE,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hr.dict_departments (
    id              SERIAL PRIMARY KEY,
    dept_id         INTEGER NOT NULL UNIQUE,
    dept_name       VARCHAR(255) NOT NULL,
    parent_dept_id  INTEGER,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Основная таблица сотрудников
-- ============================================================

DROP TABLE IF EXISTS hr.employees CASCADE;

CREATE TABLE hr.employees (
    id                  SERIAL PRIMARY KEY,
    last_name           VARCHAR(100) NOT NULL,
    first_name          VARCHAR(100) NOT NULL,
    middle_name         VARCHAR(100),
    birth_date          DATE NOT NULL,
    hire_date           DATE NOT NULL,
    termination_date    DATE,
    position            VARCHAR(100),
    salary              NUMERIC(10, 2),
    passport_data       VARCHAR(20),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_hire_after_birth  CHECK (hire_date >= birth_date),
    CONSTRAINT chk_term_after_hire   CHECK (termination_date IS NULL OR termination_date >= hire_date)
);

-- ============================================================
-- Карантин и история метрик
-- ============================================================

CREATE TABLE IF NOT EXISTS data_quality.quarantine_log (
    id                  SERIAL PRIMARY KEY,
    source_file         VARCHAR(255),
    raw_data            JSONB,
    validation_errors   JSONB,
    dq_status           VARCHAR(20) DEFAULT 'new',
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_quality.dq_metrics_history (
    id              SERIAL PRIMARY KEY,
    metric_name     VARCHAR(100),
    table_name      VARCHAR(100),
    metric_value    NUMERIC,
    status          VARCHAR(20),
    check_date      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Справочные данные
-- ============================================================

INSERT INTO hr.dict_positions (position_name, is_active)
SELECT * FROM (VALUES
    ('инженер', true),
    ('бухгалтер', true),
    ('менеджер', true),
    ('директор', true),
    ('программист', true),
    ('аналитик', true),
    ('водитель', true),
    ('разработчик', true),
    ('тестировщик', true),
    ('системный администратор', true),
    ('hr-менеджер', true),
    ('финансовый аналитик', true),
    ('юрист', true),
    ('маркетолог', true),
    ('продакт-менеджер', true)
) AS v(position_name, is_active)
WHERE NOT EXISTS (SELECT 1 FROM hr.dict_positions LIMIT 1);

INSERT INTO hr.dict_departments (dept_id, dept_name, parent_dept_id, is_active)
SELECT * FROM (VALUES
    (1, 'Руководство', NULL, true),
    (2, 'IT-департамент', 1, true),
    (3, 'Бухгалтерия', 1, true),
    (4, 'Отдел кадров', 1, true),
    (5, 'Отдел разработки', 2, true),
    (6, 'Отдел тестирования', 2, true),
    (7, 'Аналитический отдел', 2, true),
    (8, 'Финансовый отдел', 1, true),
    (9, 'Юридический отдел', 1, true),
    (10, 'Маркетинг', 1, true)
) AS v(dept_id, dept_name, parent_dept_id, is_active)
WHERE NOT EXISTS (SELECT 1 FROM hr.dict_departments LIMIT 1);

-- ============================================================
-- Индексы
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_employees_position      ON hr.employees(position);
CREATE INDEX IF NOT EXISTS idx_employees_hire_date     ON hr.employees(hire_date);
CREATE INDEX IF NOT EXISTS idx_employees_created_at    ON hr.employees(created_at);
CREATE INDEX IF NOT EXISTS idx_employees_passport      ON hr.employees(passport_data);
CREATE INDEX IF NOT EXISTS idx_quarantine_status       ON data_quality.quarantine_log(dq_status);
CREATE INDEX IF NOT EXISTS idx_quarantine_created      ON data_quality.quarantine_log(created_at);
CREATE INDEX IF NOT EXISTS idx_dict_positions_name     ON hr.dict_positions(position_name);
CREATE INDEX IF NOT EXISTS idx_dict_departments_name   ON hr.dict_departments(dept_name);

-- ============================================================
-- Триггеры для updated_at
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_dict_positions_updated_at ON hr.dict_positions;
CREATE TRIGGER update_dict_positions_updated_at
    BEFORE UPDATE ON hr.dict_positions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_dict_departments_updated_at ON hr.dict_departments;
CREATE TRIGGER update_dict_departments_updated_at
    BEFORE UPDATE ON hr.dict_departments
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
