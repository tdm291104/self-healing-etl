#!/bin/bash
set -e

echo "==> Creating databases and users..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Airflow metadata DB
    CREATE USER airflow WITH PASSWORD 'airflow';
    CREATE DATABASE airflow_db OWNER airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;

    -- Data warehouse DB
    CREATE USER dbt_user WITH PASSWORD 'dbt_password';
    CREATE DATABASE crypto_dw;
    GRANT ALL PRIVILEGES ON DATABASE crypto_dw TO dbt_user;
    -- Airflow also needs write access to load raw data
    GRANT ALL PRIVILEGES ON DATABASE crypto_dw TO airflow;
EOSQL

echo "==> Setting up crypto_dw schemas and tables..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "crypto_dw" <<-EOSQL
    -- Schemas
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS marts;
    CREATE SCHEMA IF NOT EXISTS monitoring;

    -- Grant schema-level access
    GRANT ALL ON SCHEMA raw       TO dbt_user, airflow;
    GRANT ALL ON SCHEMA staging   TO dbt_user, airflow;
    GRANT ALL ON SCHEMA marts     TO dbt_user, airflow;
    GRANT ALL ON SCHEMA monitoring TO dbt_user, airflow;

    ALTER DEFAULT PRIVILEGES IN SCHEMA raw       GRANT ALL ON TABLES    TO dbt_user, airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA staging   GRANT ALL ON TABLES    TO dbt_user, airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA marts     GRANT ALL ON TABLES    TO dbt_user, airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring GRANT ALL ON TABLES   TO dbt_user, airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA raw       GRANT ALL ON SEQUENCES TO dbt_user, airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA staging   GRANT ALL ON SEQUENCES TO dbt_user, airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA marts     GRANT ALL ON SEQUENCES TO dbt_user, airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring GRANT ALL ON SEQUENCES TO dbt_user, airflow;

    -- -------------------------------------------------------
    -- Raw layer tables
    -- -------------------------------------------------------
    CREATE TABLE IF NOT EXISTS raw.trades (
        id            BIGSERIAL PRIMARY KEY,
        user_id       VARCHAR(50)  NOT NULL,
        coin          VARCHAR(20)  NOT NULL,
        side          VARCHAR(10),
        quantity      NUMERIC(24, 8),
        raw_quantity  TEXT,                    -- original string before conversion
        trade_time    TIMESTAMP    NOT NULL,
        source_file   VARCHAR(255),
        ingested_at   TIMESTAMP    DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw.crypto_prices (
        id          BIGSERIAL PRIMARY KEY,
        coin        VARCHAR(20)   NOT NULL,
        price_usd   NUMERIC(24, 8),
        fetched_at  TIMESTAMP     NOT NULL,
        source      VARCHAR(50)   DEFAULT 'coingecko'
    );

    -- -------------------------------------------------------
    -- Monitoring / metadata tables (used by agent in Phase 3)
    -- -------------------------------------------------------

    -- Agent compares live schema against this table to detect drift
    CREATE TABLE IF NOT EXISTS monitoring.expected_schemas (
        table_name   VARCHAR(100) NOT NULL,
        column_name  VARCHAR(100) NOT NULL,
        data_type    VARCHAR(100) NOT NULL,
        is_nullable  BOOLEAN      DEFAULT TRUE,
        updated_at   TIMESTAMP    DEFAULT NOW(),
        PRIMARY KEY (table_name, column_name)
    );

    -- Audit trail of every agent decision
    CREATE TABLE IF NOT EXISTS monitoring.agent_actions (
        id               BIGSERIAL PRIMARY KEY,
        timestamp        TIMESTAMP DEFAULT NOW(),
        trigger_type     VARCHAR(50),
        diagnosis        TEXT,
        decision         VARCHAR(50),   -- auto_fix | needs_approval | escalate
        action_taken     TEXT,
        confidence_score NUMERIC(5, 4),
        human_feedback   VARCHAR(20)    -- approved | rejected | null
    );

    -- Results of data quality checks run by Airflow / agent
    CREATE TABLE IF NOT EXISTS monitoring.dq_check_results (
        id          BIGSERIAL PRIMARY KEY,
        table_name  VARCHAR(100),
        check_name  VARCHAR(100),
        status      VARCHAR(20),        -- pass | fail | warning
        details     JSONB,
        timestamp   TIMESTAMP DEFAULT NOW()
    );

    -- -------------------------------------------------------
    -- Seed: expected schema for raw.trades
    -- Agent uses this in Phase 3 to detect schema drift
    -- -------------------------------------------------------
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw        TO dbt_user, airflow;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA monitoring TO dbt_user, airflow;

    INSERT INTO monitoring.expected_schemas (table_name, column_name, data_type, is_nullable)
    VALUES
        ('raw.trades', 'user_id',    'character varying', false),
        ('raw.trades', 'coin',       'character varying', false),
        ('raw.trades', 'side',       'character varying', true),
        ('raw.trades', 'quantity',   'numeric',           true),
        ('raw.trades', 'trade_time', 'timestamp without time zone', false)
    ON CONFLICT (table_name, column_name) DO NOTHING;
EOSQL

echo "==> Database initialisation complete."
