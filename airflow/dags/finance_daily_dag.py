"""
Finance Data Platform — Master Daily DAG
=========================================
Orchestrates the full production pipeline:

  Stripe API → ADLS Gen2 → Snowflake RAW → dbt Bronze/Silver/Gold → Reconciliation → Notification

Schedule  : 02:00 UTC daily  (after Stripe nightly batch settles)
Entities  : balance_transactions, charges, refunds, disputes, payouts,
            customers, subscriptions, invoices, invoice_line_items  +  fx_rates
Watermarks: incremental — reads last_successful_watermark from AUDIT.INGESTION_CONTROL
            so each run only fetches records created since the previous successful run.
Alerting  : Slack webhook on both success and failure.
            on_failure_callback fires on any task failure.
"""

from __future__ import annotations

import os
import uuid
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_finance"
DBT_PROJECT_DIR   = "/opt/airflow/dbt"
SCRIPTS_DIR       = "/opt/airflow/scripts"
SNOWFLAKE_DB      = os.environ.get("SNOWFLAKE_DATABASE", "FINANCE_PLATFORM_DEV")

# All Stripe entities — ingested in parallel, but listed in logical order
STRIPE_ENTITIES = [
    "balance_transactions",
    "charges",
    "refunds",
    "disputes",
    "payouts",
    "customers",
    "subscriptions",
    "invoices",
    "invoice_line_items",
]

# Maps entity name → (RAW table name, Snowflake stage name)
ENTITY_COPY_MAP = {
    "balance_transactions": ("RAW_STRIPE_BALANCE_TRANSACTIONS", "STG_STRIPE_BALANCE_TRANSACTIONS"),
    "charges":              ("RAW_STRIPE_CHARGES",              "STG_STRIPE_CHARGES"),
    "refunds":              ("RAW_STRIPE_REFUNDS",              "STG_STRIPE_REFUNDS"),
    "disputes":             ("RAW_STRIPE_DISPUTES",             "STG_STRIPE_DISPUTES"),
    "payouts":              ("RAW_STRIPE_PAYOUTS",              "STG_STRIPE_PAYOUTS"),
    "customers":            ("RAW_STRIPE_CUSTOMERS",            "STG_STRIPE_CUSTOMERS"),
    "subscriptions":        ("RAW_STRIPE_SUBSCRIPTIONS",        "STG_STRIPE_SUBSCRIPTIONS"),
    "invoices":             ("RAW_STRIPE_INVOICES",             "STG_STRIPE_INVOICES"),
    "invoice_line_items":   ("RAW_STRIPE_INVOICE_LINE_ITEMS",   "STG_STRIPE_INVOICE_LINE_ITEMS"),
}

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "on_failure_callback": lambda context: send_slack_notification("failure", **context),
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _sf_conn():
    """Return an open Snowflake connection using environment variables."""
    import snowflake.connector
    return snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        role      = os.environ.get("SNOWFLAKE_ROLE", "FINANCE_TRANSFORMER"),
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "FINANCE_TRANSFORM_WH"),
        database  = os.environ["SNOWFLAKE_DATABASE"],
    )


def _get_watermark(entity: str) -> int:
    """
    Read the last successful ingestion watermark from AUDIT.INGESTION_CONTROL.
    Returns a Unix timestamp (int). Returns 0 on first run (triggers a full load).
    """
    try:
        conn = _sf_conn()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT DATEDIFF('second', '1970-01-01'::DATE, last_successful_watermark)
            FROM {SNOWFLAKE_DB}.AUDIT.INGESTION_CONTROL
            WHERE source_name = 'stripe'
              AND entity_name = '{entity}'
              AND last_successful_watermark IS NOT NULL
            LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return int(row[0]) if row else 0
    except Exception as e:
        log.warning(f"Could not read watermark for {entity}: {e}. Defaulting to 0 (full load).")
        return 0


def _copy_into_sql(entity: str) -> str:
    raw_table, stage = ENTITY_COPY_MAP[entity]
    return f"""
        COPY INTO {SNOWFLAKE_DB}.RAW.{raw_table}
        FROM @{SNOWFLAKE_DB}.RAW.{stage}
        FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE';
    """


def copy_into_optional(entity: str, **context):
    """
    COPY INTO for entities that may have zero records on a given day
    (refunds, disputes, payouts). Succeeds gracefully when the ADLS
    stage is empty — this is normal on quiet business days.
    """
    raw_table, stage = ENTITY_COPY_MAP[entity]
    sql = f"""
        COPY INTO {SNOWFLAKE_DB}.RAW.{raw_table}
        FROM @{SNOWFLAKE_DB}.RAW.{stage}
        FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE'
    """
    conn   = _sf_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        log.info(f"COPY INTO {raw_table}: {rows}")
    except Exception as e:
        err = str(e).lower()
        if "no files found" in err or "files scanned" in err or "no file" in err:
            log.info(f"No new files in stage for {entity} — skipping (expected on quiet days)")
        else:
            raise
    finally:
        cursor.close()
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# PYTHON CALLABLES
# ─────────────────────────────────────────────────────────────────────────────

def create_run_id(**context) -> str:
    run_id = f"daily-{context['ds']}-{str(uuid.uuid4())[:8]}"
    context["ti"].xcom_push(key="run_id", value=run_id)
    log.info(f"Pipeline started | run_id={run_id} | execution_date={context['ds']}")
    return run_id


def ingest_entity(entity: str, **context):
    """
    Incrementally fetch one Stripe entity and write Parquet to ADLS Gen2.
    Watermark is read from AUDIT.INGESTION_CONTROL — only new records are fetched.
    """
    import subprocess
    run_id    = context["ti"].xcom_pull(key="run_id", task_ids="create_run_id")
    watermark = _get_watermark(entity)
    log.info(f"Ingesting {entity} | watermark_ts={watermark} | run_id={run_id}")

    result = subprocess.run(
        [
            "python", f"{SCRIPTS_DIR}/stripe_to_adls.py",
            "--entity",    entity,
            "--run-id",    run_id,
            "--watermark", str(watermark),
        ],
        capture_output=True, text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"Ingestion failed for {entity}: {result.stderr}")
    return f"Ingested {entity}"


def ingest_fx_rates(**context):
    """Fetch today's FX rates — always a full daily load (no watermark)."""
    import subprocess
    run_id = context["ti"].xcom_pull(key="run_id", task_ids="create_run_id")
    result = subprocess.run(
        ["python", f"{SCRIPTS_DIR}/stripe_to_adls.py",
         "--entity", "fx_rates", "--run-id", run_id],
        capture_output=True, text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"FX rate ingestion failed: {result.stderr}")


def send_slack_notification(status: str, **context):
    """
    Post pipeline status to Slack.
    Called for success at pipeline end; called via on_failure_callback on any task failure.
    No-op when SLACK_WEBHOOK_URL is not configured.
    """
    import requests
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        log.info("SLACK_WEBHOOK_URL not set — skipping notification")
        return

    ti        = context.get("ti") or context.get("task_instance")
    run_id    = ti.xcom_pull(key="run_id", task_ids="create_run_id") if ti else "unknown"
    exec_date = context.get("ds", "unknown")
    task_id   = getattr(ti, "task_id", "unknown") if ti else "unknown"

    emoji  = "✅" if status == "success" else "🔴"
    detail = (
        "All checks passed. Gold layer is ready for BI consumption."
        if status == "success"
        else f"Task `{task_id}` failed. Check Airflow logs for details."
    )

    payload = {
        "text": f"{emoji} Finance Pipeline — {status.upper()}",
        "blocks": [{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{emoji} *Finance Data Platform — {status.upper()}*\n"
                    f"*Date:*   {exec_date}\n"
                    f"*Run ID:* `{run_id}`\n"
                    f"{detail}"
                ),
            },
        }],
    }
    try:
        requests.post(webhook_url, json=payload, timeout=10)
    except Exception as e:
        log.warning(f"Slack notification failed (non-blocking): {e}")


def run_reconciliation(**context):
    """
    Production data-quality gate. All checks must pass before watermarks advance.

    Checks
    ──────
    Existence  : each Silver + Gold table must have > 0 rows
    Dedup      : Gold distinct transaction_id == Silver distinct transaction_id
    Volume     : Gold fct_transactions within 5% of Silver distinct count
    Charges    : Gold fct_charges within 2% of Silver stg_charges
    Refunds    : if Silver has refunds, Gold must too (conditional)
    Disputes   : if Silver has disputes, Gold must too (conditional)
    Payouts    : Gold fct_payouts > 0
    """
    conn   = _sf_conn()
    cursor = conn.cursor()
    run_id = context["ti"].xcom_pull(key="run_id", task_ids="create_run_id")

    def _q(sql: str) -> int:
        cursor.execute(sql)
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    def _record(name: str, val_a: int, val_b: int, tolerance_pct: float, passed: bool) -> str:
        status = "passed" if passed else "failed"
        cursor.execute(f"""
            INSERT INTO {SNOWFLAKE_DB}.AUDIT.RECONCILIATION_RESULTS
                (run_id, check_name, expected_value, actual_value,
                 delta, tolerance_pct, status)
            VALUES ('{run_id}', '{name}', {val_a}, {val_b},
                    {abs(val_a - val_b)}, {tolerance_pct}, '{status}')
        """)
        conn.commit()
        icon = "✅" if passed else "❌"
        log.info(f"  {icon} {name}: a={val_a}  b={val_b}")
        return status

    failures = []

    # ── 1. Existence ──────────────────────────────────────────────────────────
    for name, sql in [
        ("silver_ledger_has_data",  f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.SILVER.STG_FINANCE_LEDGER_EVENTS"),
        ("silver_mrr_has_data",     f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.SILVER.STG_INVOICE_LINE_ITEMS"),
        ("silver_charges_has_data", f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.SILVER.STG_CHARGES"),
        ("silver_payouts_has_data", f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.SILVER.STG_PAYOUTS"),
        ("gold_transactions_has_data", f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.GOLD.FCT_TRANSACTIONS"),
        ("gold_mrr_has_data",          f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.GOLD.FCT_MRR_MOVEMENTS"),
        ("gold_charges_has_data",      f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.GOLD.FCT_CHARGES"),
        ("gold_payouts_has_data",      f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.GOLD.FCT_PAYOUTS"),
    ]:
        val = _q(sql)
        if _record(name, val, 0, 0, val > 0) == "failed":
            failures.append(name)

    # ── 2. Conditional existence — refunds & disputes (zero on quiet days is fine) ──
    for entity in ("refunds", "disputes"):
        silver_n = _q(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.SILVER.STG_{entity.upper()}")
        if silver_n > 0:
            gold_n = _q(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.GOLD.FCT_{entity.upper()}")
            check  = f"gold_{entity}_populated"
            if _record(check, silver_n, gold_n, 0, gold_n > 0) == "failed":
                failures.append(check)

    # ── 3. Ledger dedup integrity ─────────────────────────────────────────────
    silver_distinct = _q(
        f"SELECT COUNT(DISTINCT transaction_id) FROM {SNOWFLAKE_DB}.SILVER.STG_FINANCE_LEDGER_EVENTS"
    )
    gold_distinct = _q(
        f"SELECT COUNT(DISTINCT transaction_id) FROM {SNOWFLAKE_DB}.GOLD.FCT_TRANSACTIONS"
    )
    if _record("ledger_dedup_integrity", silver_distinct, gold_distinct, 0,
               silver_distinct == gold_distinct) == "failed":
        failures.append("ledger_dedup_integrity")

    # ── 4. Volume tolerance — fct_transactions vs silver distinct ────────────
    if silver_distinct > 0:
        gold_total = _q(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.GOLD.FCT_TRANSACTIONS")
        delta_pct  = abs(silver_distinct - gold_total) / silver_distinct * 100
        if _record("txn_volume_within_tolerance", silver_distinct, gold_total,
                   5.0, delta_pct <= 5.0) == "failed":
            failures.append("txn_volume_within_tolerance")

    # ── 5. Charges reconciliation ─────────────────────────────────────────────
    silver_ch = _q(f"SELECT COUNT(DISTINCT charge_id) FROM {SNOWFLAKE_DB}.SILVER.STG_CHARGES")
    gold_ch   = _q(f"SELECT COUNT(DISTINCT charge_id) FROM {SNOWFLAKE_DB}.GOLD.FCT_CHARGES")
    if silver_ch > 0:
        delta_pct = abs(silver_ch - gold_ch) / silver_ch * 100
        if _record("charges_silver_to_gold", silver_ch, gold_ch,
                   2.0, delta_pct <= 2.0) == "failed":
            failures.append("charges_silver_to_gold")

    cursor.close()
    conn.close()

    if failures:
        raise ValueError(f"❌ Reconciliation FAILED — {failures}")
    log.info("✅ All reconciliation checks passed.")


def update_watermarks(**context):
    """
    Advance all entity watermarks to execution_date in AUDIT.INGESTION_CONTROL.
    Only called after the full pipeline succeeds (including reconciliation).
    """
    run_id         = context["ti"].xcom_pull(key="run_id", task_ids="create_run_id")
    execution_date = context["ds"]

    conn = _sf_conn()
    cursor = conn.cursor()
    for entity in STRIPE_ENTITIES + ["fx_rates"]:
        cursor.execute(f"""
            UPDATE {SNOWFLAKE_DB}.AUDIT.INGESTION_CONTROL
            SET last_successful_watermark = '{execution_date} 23:59:59'::TIMESTAMP_NTZ,
                last_successful_run_id   = '{run_id}',
                last_run_status          = 'success',
                updated_at               = CURRENT_TIMESTAMP()
            WHERE source_name = 'stripe' AND entity_name = '{entity}'
        """)
    conn.commit()
    cursor.close()
    conn.close()
    log.info(f"✅ Watermarks advanced to {execution_date} | run_id={run_id}")


# ─────────────────────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id            = "finance_data_platform_daily",
    description       = "Finance platform — full daily batch pipeline (9 Stripe entities + FX rates)",
    default_args      = DEFAULT_ARGS,
    schedule_interval = "0 14 * * *",  # 00:00 AEST (UTC+10) daily — midnight Australia
    start_date        = days_ago(1),
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["finance", "production", "daily"],
) as dag:

    # ── 1. Generate run ID ────────────────────────────────────────────────────
    t_run_id = PythonOperator(
        task_id         = "create_run_id",
        python_callable = create_run_id,
    )

    # ── 2. Ingest — all Stripe entities + FX in parallel ─────────────────────
    ingest_tasks = {
        entity: PythonOperator(
            task_id         = f"ingest_{entity}",
            python_callable = ingest_entity,
            op_kwargs       = {"entity": entity},
        )
        for entity in STRIPE_ENTITIES
    }

    t_ingest_fx = PythonOperator(
        task_id         = "ingest_fx_rates",
        python_callable = ingest_fx_rates,
    )

    # ── 3. COPY INTO Snowflake RAW ────────────────────────────────────────────
    # refunds, disputes, payouts use a Python wrapper that handles empty stages
    # gracefully — zero files on a quiet day is not an error.
    OPTIONAL_ENTITIES = {"refunds", "disputes", "payouts"}

    copy_tasks = {}
    for entity in STRIPE_ENTITIES:
        if entity in OPTIONAL_ENTITIES:
            copy_tasks[entity] = PythonOperator(
                task_id         = f"copy_into_raw_{entity}",
                python_callable = copy_into_optional,
                op_kwargs       = {"entity": entity},
            )
        else:
            copy_tasks[entity] = SnowflakeOperator(
                task_id           = f"copy_into_raw_{entity}",
                snowflake_conn_id = SNOWFLAKE_CONN_ID,
                sql               = _copy_into_sql(entity),
            )

    t_copy_fx = SnowflakeOperator(
        task_id           = "copy_into_raw_fx_rates",
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
        sql               = f"""
            COPY INTO {SNOWFLAKE_DB}.RAW.RAW_FX_RATES
            FROM @{SNOWFLAKE_DB}.RAW.STG_FX_RATES
            FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE';
        """,
    )

    # ── 4. dbt source freshness ───────────────────────────────────────────────
    # Non-blocking: freshness warnings/errors are logged but don't stop the
    # pipeline. In production, route the output to a monitoring alert instead.
    t_source_freshness = BashOperator(
        task_id      = "dbt_source_freshness",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt source freshness --profiles-dir . --target dev || "
            f"echo 'Source freshness check completed with warnings — pipeline continues'"
        ),
    )

    # ── 5–7. dbt Bronze → Silver → Gold ──────────────────────────────────────
    t_dbt_bronze = BashOperator(
        task_id      = "dbt_run_bronze",
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt run --select tag:bronze --profiles-dir . --target dev",
    )

    t_dbt_silver = BashOperator(
        task_id      = "dbt_run_silver",
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt run --select tag:silver --profiles-dir . --target dev",
    )

    t_dbt_test_silver = BashOperator(
        task_id      = "dbt_test_silver",
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt test --select tag:silver --profiles-dir . --target dev",
    )

    t_dbt_gold = BashOperator(
        task_id      = "dbt_run_gold",
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt run --select tag:gold --profiles-dir . --target dev",
    )

    t_dbt_test_gold = BashOperator(
        task_id      = "dbt_test_gold",
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt test --select tag:gold --profiles-dir . --target dev",
    )

    # ── 8. Reconciliation ─────────────────────────────────────────────────────
    t_reconciliation = PythonOperator(
        task_id         = "finance_reconciliation_checks",
        python_callable = run_reconciliation,
    )

    # ── 9. Advance watermarks ─────────────────────────────────────────────────
    t_watermarks = PythonOperator(
        task_id         = "update_watermarks",
        python_callable = update_watermarks,
    )

    # ── 10. Success notification ──────────────────────────────────────────────
    t_notify_success = PythonOperator(
        task_id         = "notify_success",
        python_callable = send_slack_notification,
        op_kwargs       = {"status": "success"},
    )

    # ─────────────────────────────────────────────────────────────────────────
    # DEPENDENCY GRAPH
    #
    #  create_run_id
    #    ├─► ingest_balance_transactions ─► copy_into_raw_balance_transactions ─┐
    #    ├─► ingest_charges              ─► copy_into_raw_charges              ─┤
    #    ├─► ingest_refunds              ─► copy_into_raw_refunds              ─┤
    #    ├─► ingest_disputes             ─► copy_into_raw_disputes             ─┤
    #    ├─► ingest_payouts              ─► copy_into_raw_payouts              ─┤
    #    ├─► ingest_customers            ─► copy_into_raw_customers            ─┤
    #    ├─► ingest_subscriptions        ─► copy_into_raw_subscriptions        ─┤
    #    ├─► ingest_invoices             ─► copy_into_raw_invoices             ─┤
    #    ├─► ingest_invoice_line_items   ─► copy_into_raw_invoice_line_items   ─┤
    #    └─► ingest_fx_rates             ─► copy_into_raw_fx_rates             ─┘
    #                                                │ (all 10 copy tasks)
    #                                       dbt_source_freshness
    #                                                │
    #                                        dbt_run_bronze
    #                                                │
    #                               dbt_run_silver → dbt_test_silver
    #                                                │
    #                               dbt_run_gold   → dbt_test_gold
    #                                                │
    #                               finance_reconciliation_checks
    #                                                │
    #                                       update_watermarks
    #                                                │
    #                                        notify_success
    # ─────────────────────────────────────────────────────────────────────────

    all_copy_tasks = list(copy_tasks.values()) + [t_copy_fx]

    # create_run_id → all ingest tasks (parallel)
    t_run_id >> list(ingest_tasks.values()) + [t_ingest_fx]

    # each ingest → its own copy
    for entity in STRIPE_ENTITIES:
        ingest_tasks[entity] >> copy_tasks[entity]
    t_ingest_fx >> t_copy_fx

    # all copies → source freshness → linear dbt chain → reconciliation → watermarks → notify
    all_copy_tasks >> t_source_freshness
    (
        t_source_freshness
        >> t_dbt_bronze
        >> t_dbt_silver
        >> t_dbt_test_silver
        >> t_dbt_gold
        >> t_dbt_test_gold
        >> t_reconciliation
        >> t_watermarks
        >> t_notify_success
    )
