"""
Snowflake environment setup + CSV load (COPY INTO) for ECO_STREAM analytics project.

What this script does:
  1) Connects to Snowflake (via env vars / optional .env).
  2) Creates database ECO_STREAM, schema RAW, an internal stage, and file format.
  3) Creates raw tables (CUSTOMERS, USAGE_LOGS, BILLING).
  4) Uploads local CSVs to the internal stage using PUT.
  5) Loads data into tables using COPY INTO.

Required environment variables:
  - SNOWFLAKE_ACCOUNT
  - SNOWFLAKE_USER
  - SNOWFLAKE_PASSWORD

Optional (recommended):
  - SNOWFLAKE_ROLE
  - SNOWFLAKE_WAREHOUSE
  - SNOWFLAKE_DATABASE (defaults to ECO_STREAM)
  - SNOWFLAKE_SCHEMA (defaults to RAW)
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable

import snowflake.connector

try:
    # Optional, but convenient for local dev
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def _required_env(name: str) -> str:
    v = _env(name)
    if not v:
        raise SystemExit(f"Missing required environment variable: {name}")
    return v


def _execute_many(cur, statements: Iterable[str]) -> None:
    for s in statements:
        cur.execute(s)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create ECO_STREAM objects and load CSVs into Snowflake.")
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing customers.csv, usage_logs.csv, billing.csv (default: data)",
    )
    parser.add_argument(
        "--database",
        default=_env("SNOWFLAKE_DATABASE", "ECO_STREAM"),
        help="Target database (default: $SNOWFLAKE_DATABASE or ECO_STREAM)",
    )
    parser.add_argument(
        "--schema",
        default=_env("SNOWFLAKE_SCHEMA", "RAW"),
        help="Target schema (default: $SNOWFLAKE_SCHEMA or RAW)",
    )
    parser.add_argument(
        "--stage",
        default="SAAS_INTERNAL_STAGE",
        help="Internal stage name (default: SAAS_INTERNAL_STAGE)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="If set, TRUNCATE tables before loading.",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    customers_csv = data_dir / "customers.csv"
    usage_csv = data_dir / "usage_logs.csv"
    billing_csv = data_dir / "billing.csv"

    for p in (customers_csv, usage_csv, billing_csv):
        if not p.exists():
            raise SystemExit(
                f"Missing input file: {p}. Run `python data_generator.py --output-dir {data_dir}` first."
            )

    conn = snowflake.connector.connect(
        account=_required_env("SNOWFLAKE_ACCOUNT"),
        user=_required_env("SNOWFLAKE_USER"),
        password=_required_env("SNOWFLAKE_PASSWORD"),
        role=_env("SNOWFLAKE_ROLE"),
        warehouse=_env("SNOWFLAKE_WAREHOUSE"),
        autocommit=True,
    )

    db = args.database
    schema = args.schema
    stage = args.stage
    file_format = "CSV_INTERNAL_FORMAT"

    try:
        with conn.cursor() as cur:
            _execute_many(
                cur,
                [
                    f'CREATE DATABASE IF NOT EXISTS "{db}"',
                    f'CREATE SCHEMA IF NOT EXISTS "{db}"."{schema}"',
                    f'USE DATABASE "{db}"',
                    f'USE SCHEMA "{schema}"',
                    # File format for consistent CSV loading
                    f"""
                    CREATE FILE FORMAT IF NOT EXISTS "{file_format}"
                      TYPE = CSV
                      FIELD_DELIMITER = ','
                      SKIP_HEADER = 1
                      FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                      NULL_IF = ('', 'NULL', 'null')
                      EMPTY_FIELD_AS_NULL = TRUE
                      TRIM_SPACE = TRUE
                    """.strip(),
                    # Internal stage
                    f'CREATE STAGE IF NOT EXISTS "{stage}"',
                    # Raw tables
                    f"""
                    CREATE TABLE IF NOT EXISTS CUSTOMERS (
                      ACCOUNT_ID STRING,
                      CUSTOMER_NAME STRING,
                      PLAN_TIER STRING,
                      INDUSTRY STRING,
                      REGION STRING,
                      SIGNUP_DATE DATE
                    )
                    """.strip(),
                    f"""
                    CREATE TABLE IF NOT EXISTS USAGE_LOGS (
                      USAGE_DATE DATE,
                      ACCOUNT_ID STRING,
                      CREDITS_USED NUMBER(12,3),
                      ACTIVE_USERS NUMBER(12,0)
                    )
                    """.strip(),
                    f"""
                    CREATE TABLE IF NOT EXISTS BILLING (
                      BILLING_MONTH STRING,           -- YYYY-MM
                      ACCOUNT_ID STRING,
                      PLAN_TIER STRING,
                      CREDITS_BILLED NUMBER(12,3),
                      BASE_FEE_USD NUMBER(12,2),
                      OVERAGE_USD NUMBER(12,2),
                      AMOUNT_USD NUMBER(12,2)
                    )
                    """.strip(),
                ],
            )

            if args.replace:
                _execute_many(cur, ["TRUNCATE TABLE CUSTOMERS", "TRUNCATE TABLE USAGE_LOGS", "TRUNCATE TABLE BILLING"])

            # Upload local files to internal stage (PUT requires file:// and absolute path)
            for p in (customers_csv, usage_csv, billing_csv):
                abs_path = p.resolve().as_posix()
                cur.execute(f'PUT file://{abs_path} @"{stage}" AUTO_COMPRESS=TRUE OVERWRITE=TRUE')

            # COPY INTO from stage -> tables
            loads = [
                ("CUSTOMERS", "customers.csv"),
                ("USAGE_LOGS", "usage_logs.csv"),
                ("BILLING", "billing.csv"),
            ]

            for table, filename in loads:
                cur.execute(
                    f"""
                    COPY INTO {table}
                    FROM @"{stage}"
                    FILE_FORMAT = (FORMAT_NAME = "{file_format}")
                    PATTERN = '.*{filename}(\\.gz)?'
                    ON_ERROR = 'ABORT_STATEMENT'
                    """.strip()
                )

    finally:
        conn.close()

    print("Snowflake setup + load complete.")
    print(f'Objects created/used: "{db}"."{schema}", stage "{stage}"')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

