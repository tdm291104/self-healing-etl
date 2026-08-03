# Self-Healing ETL Pipeline
**Domain**: Crypto Trading Platform Analytics

An end-to-end ETL pipeline that ingests crypto trade data and market prices, transforms them with dbt, and uses a LangGraph AI agent to detect, diagnose, and auto-remediate data quality issues — without human intervention for common failure patterns.

---

## Architecture

```
[CSV Trades] → [raw.trades]  ─┐
                               ├─ dbt ─► [staging] ─► [fct_trades_usd_value]
[CoinGecko]  → [raw.prices] ─┘                    ─► [fct_user_portfolio_value]
                    │
         (on failure / anomaly)
                    ▼
          [LangGraph Monitoring Agent]
          ├── read_logs()
          ├── get_schema_diff()
          ├── sample_bad_rows()
          └── check_source_availability()
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
    Auto-remediate      Alert + Report
    (safe fixes)        (Telegram + Dashboard)
```

## Tech Stack

| Layer         | Tool                  |
|---------------|-----------------------|
| Orchestration | Apache Airflow 2.9    |
| Warehouse     | PostgreSQL 15         |
| Transform     | dbt-core 1.7          |
| Agent         | LangGraph + Claude API|
| Alerting      | Telegram Bot API      |
| Dashboard     | Streamlit             |
| Container     | Docker Compose        |

---

## Quick Start

```bash
# 1. Copy env file and fill in your values
cp .env.example .env

# (Optional) Generate a new Fernet key:
# python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 2. Build images and start all services
docker-compose up --build

# 3. Airflow UI → http://localhost:8080  (admin / admin)
# 4. Enable and trigger DAGs manually to test
```

---

## Database Layout

```
crypto_dw
├── raw
│   ├── trades             — daily CSV ingest
│   └── crypto_prices      — hourly CoinGecko prices
├── staging
│   ├── stg_raw_trades
│   └── stg_raw_crypto_prices
├── marts
│   ├── fct_trades_usd_value
│   └── fct_user_portfolio_value
└── monitoring
    ├── expected_schemas   — schema registry for drift detection
    ├── agent_actions      — audit trail of every agent decision
    └── dq_check_results   — data quality check history
```

---

## Roadmap

| Phase | Status | Description |
|-------|--------|-------------|
| 1 | ✅ Done | Airflow + PostgreSQL + dbt pipeline running end-to-end |
| 2 | 🔲 | Controlled error injection (schema drift, duplicates, volume anomaly) + DQ checks |
| 3 | 🔲 | LangGraph agent with full tool suite and decision engine |
| 4 | 🔲 | Streamlit dashboard showing agent audit trail |

---

## Project Structure

```
├── dags/              Airflow DAGs
├── dbt/               dbt project (models, tests, macros)
├── agent/             LangGraph agent (Phase 3)
├── dashboard/         Streamlit app (Phase 4)
├── docker/airflow/    Custom Airflow Dockerfile + requirements
├── scripts/           DB init script, CSV generator
└── data/              Generated CSV files (gitignored)
```
