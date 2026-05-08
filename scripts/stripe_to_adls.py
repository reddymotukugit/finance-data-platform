"""
Finance Data Platform — Stripe to ADLS Ingestion Script
Pulls data from Stripe API and writes Parquet files to ADLS Gen2.
Used by Airflow tasks or run standalone for testing.

Usage:
    python stripe_to_adls.py --entity balance_transactions --run-id test-001
    python stripe_to_adls.py --entity all --run-id daily-2026-05-04

Requirements:
    pip install stripe pandas pyarrow azure-storage-file-datalake python-dotenv requests
"""

import os
import uuid
import json
import logging
import argparse
from decimal import Decimal
from datetime import datetime, timezone
from dotenv import load_dotenv

import stripe
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

class StripeEncoder(json.JSONEncoder):
    """Handle Decimal and other non-serializable Stripe types."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

def stripe_json(obj) -> str:
    return json.dumps(obj, cls=StripeEncoder)

# ────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────
STRIPE_API_KEY     = os.environ["STRIPE_API_KEY"]
STORAGE_ACCOUNT    = os.environ["AZURE_STORAGE_ACCOUNT"]
STORAGE_KEY        = os.environ["AZURE_STORAGE_KEY"]
CONTAINER_NAME     = os.environ.get("AZURE_CONTAINER", "finance")

stripe.api_key = STRIPE_API_KEY

# ────────────────────────────────────────
# ADLS CLIENT
# ────────────────────────────────────────
def get_adls_client():
    return DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
        credential=STORAGE_KEY
    )


def upload_parquet_to_adls(df: pd.DataFrame, entity: str, run_id: str, adls_client):
    """Write a DataFrame as Parquet to ADLS landing zone."""
    now = datetime.now(timezone.utc)
    path = f"landing/stripe/{entity}/{now.year}/{now.month:02d}/{now.day:02d}/run_id={run_id}/part-0001.parquet"

    # Convert to parquet bytes
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy")
    data = buf.getvalue().to_pybytes()

    # Upload
    fs_client = adls_client.get_file_system_client(CONTAINER_NAME)
    file_client = fs_client.get_file_client(path)
    file_client.upload_data(data, overwrite=True)

    log.info(f"Uploaded {len(df)} rows to adls://{CONTAINER_NAME}/{path}")
    return path


# ────────────────────────────────────────
# STRIPE EXTRACTORS
# ────────────────────────────────────────

def fetch_balance_transactions(watermark_ts: int = 0) -> pd.DataFrame:
    """Fetch all balance transactions created after watermark."""
    log.info(f"Fetching balance_transactions since unix={watermark_ts}")
    records = []
    params = {"limit": 100, "created": {"gt": watermark_ts}}
    for tx in stripe.BalanceTransaction.auto_paging_iter(**params):
        records.append({
            "id":                   tx.id,
            "object":               tx.object,
            "amount":               tx.amount,
            "available_on":         tx.available_on,
            "created":              tx.created,
            "currency":             tx.currency,
            "description":          tx.description,
            "exchange_rate":        tx.exchange_rate,
            "fee":                  tx.fee,
            "net":                  tx.net,
            "reporting_category":   tx.reporting_category,
            "source":               tx.source,
            "status":               tx.status,
            "type":                 tx.type,
            "raw_payload":          stripe_json(tx.to_dict()),
        })
    return pd.DataFrame(records)


def fetch_charges(watermark_ts: int = 0) -> pd.DataFrame:
    log.info(f"Fetching charges since unix={watermark_ts}")
    records = []
    for ch in stripe.Charge.auto_paging_iter(limit=100, created={"gt": watermark_ts}):
        records.append({
            "id":               ch.id,
            "object":           getattr(ch, "object", None),
            "amount":           getattr(ch, "amount", None),
            "amount_captured":  getattr(ch, "amount_captured", None),
            "amount_refunded":  getattr(ch, "amount_refunded", None),
            "captured":         getattr(ch, "captured", None),
            "created":          getattr(ch, "created", None),
            "currency":         getattr(ch, "currency", None),
            "customer":         getattr(ch, "customer", None),
            "description":      getattr(ch, "description", None),
            "disputed":         getattr(ch, "disputed", None),
            "failure_code":     getattr(ch, "failure_code", None),
            "failure_message":  getattr(ch, "failure_message", None),
            "invoice":          getattr(ch, "invoice", None),
            "paid":             getattr(ch, "paid", None),
            "payment_intent":   getattr(ch, "payment_intent", None),
            "receipt_email":    getattr(ch, "receipt_email", None),
            "refunded":         getattr(ch, "refunded", None),
            "status":           getattr(ch, "status", None),
            "raw_payload":      stripe_json(ch.to_dict()),
        })
    return pd.DataFrame(records)


def fetch_customers(watermark_ts: int = 0) -> pd.DataFrame:
    log.info(f"Fetching customers since unix={watermark_ts}")
    records = []
    for c in stripe.Customer.auto_paging_iter(limit=100, created={"gt": watermark_ts}):
        addr = c.address or {}
        records.append({
            "id":                   c.id,
            "object":               c.object,
            "created":              c.created,
            "currency":             c.currency,
            "description":          c.description,
            "email":                c.email,
            "name":                 c.name,
            "phone":                c.phone,
            "address_city":         addr.get("city"),
            "address_country":      addr.get("country"),
            "address_line1":        addr.get("line1"),
            "address_postal_code":  addr.get("postal_code"),
            "delinquent":           c.delinquent,
            "balance":              c.balance,
            "tax_exempt":           c.tax_exempt,
            "raw_payload":          stripe_json(c.to_dict()),
        })
    return pd.DataFrame(records)


def fetch_subscriptions(watermark_ts: int = 0) -> pd.DataFrame:
    log.info(f"Fetching subscriptions since unix={watermark_ts}")
    records = []
    for s in stripe.Subscription.auto_paging_iter(limit=100, created={"gt": watermark_ts}):
        plan = getattr(s, "plan", None) or {}
        records.append({
            "id":                       s.id,
            "object":                   getattr(s, "object", None),
            "customer":                 getattr(s, "customer", None),
            "created":                  getattr(s, "created", None),
            "current_period_start":     getattr(s, "current_period_start", None),
            "current_period_end":       getattr(s, "current_period_end", None),
            "cancel_at":                getattr(s, "cancel_at", None),
            "canceled_at":              getattr(s, "canceled_at", None),
            "ended_at":                 getattr(s, "ended_at", None),
            "start_date":               getattr(s, "start_date", None),
            "status":                   getattr(s, "status", None),
            "plan_id":                  getattr(plan, "id", None),
            "plan_interval":            getattr(plan, "interval", None),
            "plan_interval_count":      getattr(plan, "interval_count", None),
            "plan_amount":              getattr(plan, "amount", None),
            "plan_currency":            getattr(plan, "currency", None),
            "quantity":                 getattr(s, "quantity", None),
            "raw_payload":              stripe_json(s.to_dict()),
        })
    return pd.DataFrame(records)


def fetch_invoices(watermark_ts: int = 0) -> pd.DataFrame:
    log.info(f"Fetching invoices since unix={watermark_ts}")
    records = []
    for inv in stripe.Invoice.auto_paging_iter(limit=100, created={"gt": watermark_ts}):
        records.append({
            "id":               inv.id,
            "object":           getattr(inv, "object", None),
            "account_country":  getattr(inv, "account_country", None),
            "amount_due":       getattr(inv, "amount_due", None),
            "amount_paid":      getattr(inv, "amount_paid", None),
            "amount_remaining": getattr(inv, "amount_remaining", None),
            "created":          getattr(inv, "created", None),
            "currency":         getattr(inv, "currency", None),
            "customer":         getattr(inv, "customer", None),
            "customer_email":   getattr(inv, "customer_email", None),
            "due_date":         getattr(inv, "due_date", None),
            "period_end":       getattr(inv, "period_end", None),
            "period_start":     getattr(inv, "period_start", None),
            "status":           getattr(inv, "status", None),
            "subscription":     getattr(inv, "subscription", None),
            "subtotal":         getattr(inv, "subtotal", None),
            "tax":              getattr(inv, "tax", None),
            "total":            getattr(inv, "total", None),
            "raw_payload":      stripe_json(inv.to_dict()),
        })
    return pd.DataFrame(records)


def fetch_invoice_line_items(watermark_ts: int = 0) -> pd.DataFrame:
    """Fetch invoice line items by iterating invoices."""
    log.info("Fetching invoice_line_items (via invoices)")
    records = []
    for inv in stripe.Invoice.auto_paging_iter(limit=100, created={"gt": watermark_ts}):
        for li in inv.lines.auto_paging_iter():
            plan  = getattr(li, "plan", None) or {}
            price = getattr(li, "price", None) or {}
            period = getattr(li, "period", None)
            records.append({
                "id":                   li.id,
                "object":               getattr(li, "object", None),
                "invoice_id":           inv.id,
                "amount":               getattr(li, "amount", None),
                "currency":             getattr(li, "currency", None),
                "description":          getattr(li, "description", None),
                "discountable":         getattr(li, "discountable", None),
                "invoice_item":         getattr(li, "invoice_item", None),
                "period_start":         getattr(period, "start", None) if period else None,
                "period_end":           getattr(period, "end", None) if period else None,
                "plan_id":              getattr(plan, "id", None),
                "plan_interval":        getattr(plan, "interval", None),
                "plan_interval_count":  getattr(plan, "interval_count", None),
                "plan_amount":          getattr(plan, "amount", None),
                "price_id":             getattr(price, "id", None),
                "proration":            getattr(li, "proration", None),
                "quantity":             getattr(li, "quantity", None),
                "subscription":         getattr(li, "subscription", None),
                "subscription_item":    getattr(li, "subscription_item", None),
                "type":                 getattr(li, "type", None),
                "raw_payload":          stripe_json(li.to_dict()),
            })
    return pd.DataFrame(records)


def fetch_fx_rates() -> pd.DataFrame:
    """Fetch today's FX rates from exchangerate.host (free API, no key needed)."""
    import requests
    log.info("Fetching FX rates from exchangerate.host")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    resp = requests.get(
        "https://api.exchangerate.host/latest",
        params={"base": "USD", "symbols": "EUR,GBP,CAD,AUD,JPY,CHF,SEK,NOK,DKK,INR"},
        timeout=30
    )
    data = resp.json()
    records = []
    for currency, rate in data.get("rates", {}).items():
        records.append({
            "rate_date":        today,
            "from_currency":    currency,
            "to_currency":      "USD",
            "rate":             round(1.0 / rate, 6) if rate else None,
            "source":           "exchangerate.host",
        })
    # Also add USD→USD = 1.0
    records.append({"rate_date": today, "from_currency": "USD", "to_currency": "USD", "rate": 1.0, "source": "exchangerate.host"})
    return pd.DataFrame(records)


# ────────────────────────────────────────
# ENTITY MAP
# ────────────────────────────────────────
ENTITY_FETCHERS = {
    "balance_transactions": fetch_balance_transactions,
    "charges":              fetch_charges,
    "customers":            fetch_customers,
    "subscriptions":        fetch_subscriptions,
    "invoices":             fetch_invoices,
    "invoice_line_items":   fetch_invoice_line_items,
}

FX_ONLY_ENTITIES = {"fx_rates": fetch_fx_rates}


# ────────────────────────────────────────
# MAIN
# ────────────────────────────────────────
def run_ingestion(entity: str, run_id: str, watermark_ts: int = 0):
    adls = get_adls_client()

    if entity == "all":
        entities = list(ENTITY_FETCHERS.keys())
    else:
        entities = [entity]

    results = {}
    for ent in entities:
        try:
            if ent in ENTITY_FETCHERS:
                df = ENTITY_FETCHERS[ent](watermark_ts)
            elif ent == "fx_rates":
                df = fetch_fx_rates()
            else:
                log.warning(f"Unknown entity: {ent}")
                continue

            if df.empty:
                log.info(f"No new records for {ent}")
                results[ent] = 0
                continue

            df["ingestion_run_id"] = run_id
            df["load_mode"]        = "batch"
            df["loaded_at"]        = datetime.now(timezone.utc).isoformat()

            path = upload_parquet_to_adls(df, ent, run_id, adls)
            results[ent] = len(df)
            log.info(f"SUCCESS: {ent} — {len(df)} records → {path}")

        except Exception as e:
            log.error(f"FAILED: {ent} — {e}")
            results[ent] = -1

    # Run FX rates as well
    try:
        df_fx = fetch_fx_rates()
        df_fx["ingestion_run_id"] = run_id
        df_fx["load_mode"]        = "batch"
        df_fx["loaded_at"]        = datetime.now(timezone.utc).isoformat()
        upload_parquet_to_adls(df_fx, "fx/rates", run_id, adls)
        results["fx_rates"] = len(df_fx)
        log.info(f"SUCCESS: fx_rates — {len(df_fx)} records")
    except Exception as e:
        log.error(f"FAILED: fx_rates — {e}")

    print("\n" + "="*50)
    print("INGESTION SUMMARY")
    print("="*50)
    for ent, count in results.items():
        status = "OK" if count >= 0 else "FAIL"
        print(f"  {status:4s}  {ent:<30} {count if count >= 0 else 'ERROR'} records")
    print("="*50)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stripe → ADLS ingestion")
    parser.add_argument("--entity", default="all",
        help="Entity to ingest (balance_transactions, charges, customers, subscriptions, invoices, invoice_line_items, fx_rates, all)")
    parser.add_argument("--run-id", default=str(uuid.uuid4())[:8],
        help="Unique run identifier")
    parser.add_argument("--watermark", type=int, default=0,
        help="Unix timestamp — only fetch records created after this time")
    args = parser.parse_args()

    log.info(f"Starting ingestion | entity={args.entity} run_id={args.run_id} watermark={args.watermark}")
    run_ingestion(args.entity, args.run_id, args.watermark)
