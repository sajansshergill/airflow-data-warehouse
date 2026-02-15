from __future__ import annotations

import os
import time
import json
import requests
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context

import psycopg2
from psycopg2.extras import execute_values, Json


# -------------------------
# Config
# -------------------------
DAG_ID = "crypto_to_postgres_warehouse"
PIPELINE_NAME = "crypto_hourly_prices"

ASSETS = [
    {"asset_id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
    {"asset_id": "ethereum", "symbol": "eth", "name": "Ethereum"},
]

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/{id}/market_chart/range"


def mark_audit_failed(context):
    """On DAG/task failure: update etl_run_audit to status=failed."""
    try:
        from airflow.models import XCom

        dag_run = context.get("dag_run")
        if not dag_run:
            return
        # Our audit uses run_id from audit_start (e.g. "2026-02-14_to_2026-02-15"), not dag_run.run_id
        audit_ctx = XCom.get_one(
            run_id=dag_run.run_id,
            task_ids="audit_start",
            dag_id=DAG_ID,
        )
        our_run_id = audit_ctx.get("run_id") if isinstance(audit_ctx, dict) else None
        if not our_run_id:
            return
        exc = str(context.get("exception", "Unknown"))[:5000]
        conn = get_warehouse_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE etl_run_audit
                    SET status = 'failed', error = %s, updated_at = NOW()
                    WHERE dag_id = %s AND run_id = %s;
                    """,
                    (exc, DAG_ID, our_run_id),
                )
        conn.close()
    except Exception:
        pass


def get_warehouse_conn():
    host = os.getenv("WAREHOUSE_HOST", "localhost")
    port = int(os.getenv("WAREHOUSE_PORT", "5432"))
    db = os.getenv("WAREHOUSE_DB", "warehouse")
    user = os.getenv("WAREHOUSE_USER", "warehouse")
    password = os.getenv("WAREHOUSE_PASSWORD", "warehouse")
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)


def load_sql_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def to_unix(dt: datetime) -> int:
    return int(dt.timestamp())


def fetch_api_json(asset_id: str, start_dt: datetime, end_dt: datetime) -> dict:
    params = {"vs_currency": "usd", "from": to_unix(start_dt), "to": to_unix(end_dt)}
    url = COINGECKO_URL.format(id=asset_id)
    last_error = None
    for attempt in range(5):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            last_error = f"CoinGecko {r.status_code}: {r.text[:200]}"
            time.sleep(2**attempt)
            continue
        if r.status_code != 200:
            raise AirflowFailException(f"CoinGecko error {r.status_code}: {r.text[:200]}")
        return r.json()
    raise AirflowFailException(f"CoinGecko failed after retries: {last_error}")


def transform_hourly_prices(asset_id: str, api_json: dict) -> list[tuple[str, str, float]]:
    """
    Returns rows: (asset_id, ts_iso_utc, price_usd)
    - Rounds timestamps to hour
    - Keeps last price per hour
    """
    prices = api_json.get("prices", [])
    hourly: dict[datetime, float] = {}

    for ts_ms, price in prices:
        ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        ts_hour = ts.replace(minute=0, second=0, microsecond=0)
        hourly[ts_hour] = float(price)

    rows = [(asset_id, ts.isoformat(), p) for ts, p in sorted(hourly.items())]
    return rows


# -------------------------
# Default args
# -------------------------
default_args = {
    "owner": "sajan",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": mark_audit_failed,
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Extract hourly crypto prices from CoinGecko and load into Postgres warehouse (raw+audit+dq)",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "crypto", "warehouse"],
) as dag:

    @task
    def create_tables():
        sql = load_sql_file("/opt/airflow/sql/00_create_tables.sql")
        conn = get_warehouse_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
        finally:
            conn.close()

    @task
    def upsert_dim_assets():
        conn = get_warehouse_conn()
        rows = [(a["asset_id"], a["symbol"], a["name"]) for a in ASSETS]
        sql = """
        INSERT INTO dim_crypto_asset(asset_id, symbol, name)
        VALUES %s
        ON CONFLICT (asset_id) DO UPDATE
        SET symbol = EXCLUDED.symbol,
            name = EXCLUDED.name;
        """
        try:
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, rows)
        finally:
            conn.close()

    @task
    def audit_start(execution_date_str: str) -> dict:
        """
        Defines the data window (yesterday UTC) + creates an audit row.
        """
        exec_dt = datetime.fromisoformat(execution_date_str).replace(tzinfo=timezone.utc)
        data_end = exec_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        data_start = data_end - timedelta(days=1)

        run_id = f"{data_start.date().isoformat()}_to_{data_end.date().isoformat()}"

        conn = get_warehouse_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO etl_run_audit(dag_id, run_id, data_start_utc, data_end_utc, status)
                        VALUES (%s, %s, %s, %s, 'running')
                        ON CONFLICT (dag_id, run_id) DO UPDATE
                        SET data_start_utc = EXCLUDED.data_start_utc,
                            data_end_utc   = EXCLUDED.data_end_utc,
                            status         = 'running',
                            error          = NULL,
                            updated_at     = NOW();
                        """,
                        (DAG_ID, run_id, data_start, data_end),
                    )
        finally:
            conn.close()

        return {
            "run_id": run_id,
            "data_start_utc": data_start.isoformat(),
            "data_end_utc": data_end.isoformat(),
        }

    @task
    def extract_stage_transform(ctx: dict) -> dict:
        """
        Extract API JSON for each asset, stage raw JSON, then transform to hourly rows.
        """
        run_id = ctx["run_id"]
        start_dt = datetime.fromisoformat(ctx["data_start_utc"]).replace(tzinfo=timezone.utc)
        end_dt = datetime.fromisoformat(ctx["data_end_utc"]).replace(tzinfo=timezone.utc)

        all_rows: list[tuple[str, str, float]] = []
        api_latency_ms = 0

        # store raw payloads in Postgres (replayable)
        conn = get_warehouse_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    for asset in ASSETS:
                        t0 = time.time()
                        api_json = fetch_api_json(asset["asset_id"], start_dt, end_dt)
                        api_latency_ms += int((time.time() - t0) * 1000)

                        # Stage raw payload
                        cur.execute(
                            """
                            INSERT INTO raw_crypto_api_payload(run_id, asset_id, start_utc, end_utc, payload_json)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (run_id, asset_id) DO UPDATE
                            SET payload_json = EXCLUDED.payload_json,
                                extracted_at = NOW();
                            """,
                            (run_id, asset["asset_id"], start_dt, end_dt, Json(api_json)),
                        )

                        # Transform
                        rows = transform_hourly_prices(asset["asset_id"], api_json)
                        all_rows.extend(rows)
        finally:
            conn.close()

        if not all_rows:
            raise AirflowFailException("No rows extracted/transformed.")

        return {
            **ctx,
            "rows": all_rows,
            "api_latency_ms": api_latency_ms,
            "extracted_rows": len(all_rows),
        }

    @task
    def load_fact(payload: dict) -> dict:
        rows = payload["rows"]

        # (asset_id, ts, price_usd)
        db_rows = [(asset_id, ts_iso, price) for asset_id, ts_iso, price in rows]

        conn = get_warehouse_conn()
        upsert_sql = """
        INSERT INTO fact_crypto_price_hourly(asset_id, ts, price_usd)
        VALUES %s
        ON CONFLICT (asset_id, ts) DO UPDATE
        SET price_usd = EXCLUDED.price_usd,
            load_ts = NOW();
        """
        try:
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, upsert_sql, db_rows, page_size=500)
        finally:
            conn.close()

        payload["loaded_rows"] = len(db_rows)
        # Strip large "rows" before XCom to avoid serialization issues
        out = {k: v for k, v in payload.items() if k != "rows"}
        return out

    @task
    def data_quality_checks(payload: dict):
        """
        DQ checks:
        - dims populated
        - no nulls in PK / required cols
        - uniqueness enforced by PK (sanity check counts per (asset_id,ts))
        - freshness check: max(ts) >= end_utc - 2 hours
        """
        # Fallback: payload can be None if XCom fails; use data_interval_end from context
        if payload and payload.get("data_end_utc"):
            end_dt = datetime.fromisoformat(payload["data_end_utc"]).replace(tzinfo=timezone.utc)
        else:
            ctx = get_current_context()
            end_dt = ctx.get("data_interval_end") or ctx.get("logical_date")
            if end_dt:
                end_dt = end_dt.replace(tzinfo=timezone.utc) if end_dt.tzinfo is None else end_dt
            else:
                raise AirflowFailException("data_quality_checks: no payload and no data_interval_end in context")
        freshness_threshold = end_dt - timedelta(hours=2)

        conn = get_warehouse_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM dim_crypto_asset;")
                    dim_count = cur.fetchone()[0]

                    cur.execute("SELECT COUNT(*) FROM fact_crypto_price_hourly;")
                    fact_count = cur.fetchone()[0]

                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fact_crypto_price_hourly
                        WHERE asset_id IS NULL OR ts IS NULL OR price_usd IS NULL;
                    """)
                    null_violations = cur.fetchone()[0]

                    # duplicates sanity (should be 0 because PK, but useful if schema ever changes)
                    cur.execute("""
                        SELECT COUNT(*) FROM (
                          SELECT asset_id, ts, COUNT(*) c
                          FROM fact_crypto_price_hourly
                          GROUP BY asset_id, ts
                          HAVING COUNT(*) > 1
                        ) t;
                    """)
                    dup_groups = cur.fetchone()[0]

                    cur.execute("SELECT MAX(ts) FROM fact_crypto_price_hourly;")
                    max_ts = cur.fetchone()[0]

            if dim_count < len(ASSETS):
                raise AirflowFailException(f"DQ fail: dim_crypto_asset too small: {dim_count}")

            if fact_count < 10:
                raise AirflowFailException(f"DQ fail: fact_crypto_price_hourly too small: {fact_count}")

            if null_violations != 0:
                raise AirflowFailException(f"DQ fail: null violations found: {null_violations}")

            if dup_groups != 0:
                raise AirflowFailException(f"DQ fail: duplicate (asset_id,ts) groups: {dup_groups}")

            if max_ts is None or max_ts < freshness_threshold:
                raise AirflowFailException(
                    f"DQ fail: freshness check failed. max_ts={max_ts}, threshold={freshness_threshold}"
                )
        finally:
            conn.close()

    @task
    def audit_finish(payload: dict):
        """
        Mark audit success with metrics.
        """
        conn = get_warehouse_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE etl_run_audit
                        SET extracted_rows = %s,
                            loaded_rows = %s,
                            api_latency_ms = %s,
                            status = 'success',
                            updated_at = NOW()
                        WHERE dag_id = %s AND run_id = %s;
                        """,
                        (
                            int(payload.get("extracted_rows", 0)),
                            int(payload.get("loaded_rows", 0)),
                            int(payload.get("api_latency_ms", 0)),
                            DAG_ID,
                            payload["run_id"],
                        ),
                    )
        finally:
            conn.close()

    @task
    def audit_fail(ctx: dict, error: str):
        """
        Mark audit failed. (Call this manually if you want; Airflow callbacks are another option.)
        """
        conn = get_warehouse_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE etl_run_audit
                        SET status='failed',
                            error=%s,
                            updated_at=NOW()
                        WHERE dag_id=%s AND run_id=%s;
                        """,
                        (error[:5000], DAG_ID, ctx["run_id"]),
                    )
        finally:
            conn.close()

    # ---- Wiring ----
    t_create = create_tables()
    t_dim = upsert_dim_assets()
    t_start = audit_start(execution_date_str="{{ ds }}T00:00:00")
    t_extract = extract_stage_transform(t_start)
    t_load = load_fact(t_extract)
    t_dq = data_quality_checks(t_load)
    t_finish = audit_finish(t_load)

    t_create >> t_dim >> t_start >> t_extract >> t_load >> t_dq >> t_finish
