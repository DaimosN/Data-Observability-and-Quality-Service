import psycopg2
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://dq_user:dq_pass@localhost:5432/dq_db")


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)
