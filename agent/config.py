import os

DB_HOST = os.getenv('DBT_DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DBT_DB_PORT', '5432'))
DB_NAME = os.getenv('DBT_DB_NAME', 'crypto_dw')
DB_USER = os.getenv('DBT_DB_USER', 'dbt_user')
DB_PASSWORD = os.getenv('DBT_DB_PASSWORD', 'dbt_password')

ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
DATA_DIR = os.getenv('DATA_DIR', '/opt/airflow/data')
