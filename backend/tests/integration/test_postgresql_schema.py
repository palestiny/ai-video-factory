"""Real-PostgreSQL smoke test for the initial durable-work schema.

The CI workflow provides an isolated PostgreSQL database. The migration is
intentionally applied to a clean database; adapter behavior is tested later.
"""
from pathlib import Path
import os

import psycopg


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


def test_durable_work_migration_applies_and_creates_required_tables():
    database_url = os.environ["DATABASE_URL"]
    sql = MIGRATION.read_text(encoding="utf-8")

    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql)
        rows = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            """
        ).fetchall()

    tables = {row[0] for row in rows}
    assert {
        "generation_jobs",
        "idempotency_reservations",
        "generation_attempt_versions",
        "provider_operations",
        "job_events",
        "generation_work_items",
    } <= tables
