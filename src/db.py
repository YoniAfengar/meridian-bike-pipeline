import os

import psycopg


def connect():
    return psycopg.connect(
        host=os.getenv("DB_HOST", "db"),
        dbname=os.getenv("DB_NAME", "meridian"),
        user=os.getenv("DB_USER", "meridian"),
        password=os.getenv("DB_PASSWORD", "meridian"),
    )
