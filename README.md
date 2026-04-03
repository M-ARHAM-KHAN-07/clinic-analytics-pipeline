# 🦷 Clinic Analytics Pipeline

> A unified data warehouse for a 50+ clinic dental chain — consolidating Google Ads, Facebook Ads, QuickBooks, patient records, and operational targets into a single PostgreSQL database on Azure, powering cross-clinic analytics dashboards.

---

## Overview

| Metric | Value |
|---|---|
| Clinics | 50+ |
| Data sources | 6 (Google Ads, Facebook Ads, QuickBooks, Akitu API, SecureEDMS, Azure Blob) |
| ETL pipelines | 4 custom Python pipelines |
| Fivetran connectors | 3 (self-configured) |
| Warehouse | PostgreSQL 14 on Azure Database |
| Destination | `crm_data` schema → analytics dashboards |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                         │
│  Google Ads  │  Facebook Ads  │  QuickBooks  │  Akitu API  │
│  SecureEDMS portal  │  Azure Blob Storage (CSV)            │
└──────────────────────┬──────────────────────────────────────┘
                       │
          ┌────────────┴────────────┐
          │                         │
     Fivetran (managed)      Custom Python ETL
     Google Ads schema        (requests, Selenium,
     Facebook Ads schema       azure-storage-blob)
          │                         │
          └────────────┬────────────┘
                       │
         ┌─────────────▼──────────────┐
         │   Staging / raw schemas    │
         │  google_ads, facebook_ads, │
         │  public (EDMS)             │
         └─────────────┬──────────────┘
                       │
         LEFT JOIN crm_data.fb_google_clinics_mapping
         COALESCE(mapped_name, original_name)
                       │
         ┌─────────────▼──────────────┐
         │   crm_data schema          │
         │   PostgreSQL on Azure      │
         └─────────────┬──────────────┘
                       │
              Analytics dashboards
               (50+ clinic views)
```

---

## Fivetran Setup

Three Fivetran connectors configured from scratch:

| Connector | Destination Schema | Key tables |
|---|---|---|
| **Google Ads** | `google_ads` | account_history, account_stats, campaign data |
| **Facebook Ads** | `facebook_ads_main` | account_history, basic_ad, campaign stats |
| **QuickBooks** | *(financial schema)* | Revenue, expenses — enables ad spend vs. revenue analysis |

Each connector required: OAuth credential setup, destination schema configuration, column selection, and sync scheduling.

---

## Pipeline 1 — Ads Incremental Sync

**File:** `pipelines/incremental_ads_to_crm.py`

Moves data from Fivetran-synced schemas into the `crm_data` warehouse using watermark-based incremental loading — no full table scans on every run.

### How it works

```python
# 1. Read current high-water mark from target
last_wm = get_max_watermark(cur, target_schema, target_table, wm_col)

# 2. Only fetch rows newer than watermark
if last_wm:
    select_query += f" WHERE s.{wm_col} > %s"   # incremental
else:
    pass  # first run — full load

# 3. Upsert with per-table conflict key
INSERT INTO crm_data.target_table (...)
SELECT ... FROM source s
LEFT JOIN crm_data.fb_google_clinics_mapping m ON m.old_name = s.descriptive_name
ON CONFLICT (conflict_cols) DO NOTHING;
```

### Table config (each table carries its own keys)

```python
{
    "source_schema": "google_ads",
    "source_table": "account_stats",
    "target_schema": "crm_data",
    "target_table": "google_account_stats",
    "conflict_cols": ["customer_id", "date", "_fivetran_id"],  # actual PK from DDL
    "watermark_col": "date",
},
```

---

## Pipeline 2 — Patient Attrition API

**File:** `pipelines/akitu_patient_attrition.py`

Daily pipeline that logs into the Akitu practice management API, fetches today's new and lost patient counts, and upserts to Postgres.

```
Login (session auth) → POST attrition report → extract counts → upsert (date PK)
```

- Idempotent — re-running the same day updates rather than duplicates
- UTC date range built dynamically at runtime

---

## Pipeline 3 — Azure Blob CSV ETL

**File:** `pipelines/azure_blob_etl.py`

Reads clinic target/goal CSVs uploaded to Azure Blob Storage, loads them to Postgres, then archives processed files.

### Fuzzy column mapping

CSVs from 50+ clinics used inconsistent column names. Solved with alias matching:

```python
def find_col(possible_names):
    for name in possible_names:
        for i, h in enumerate(header):
            if name in h:
                return i
    return None

idx = {
    "clinic":  find_col(["clinic", "department", "office"]),
    "annual":  find_col(["annual", "year_target"]),
    "monthly": find_col(["monthly", "month_target"]),
    "weekly":  find_col(["weekly", "week_target"]),
}
```

### Archive pattern

After successful load: copy blob to `archive/filename.csv` → verify copy status → delete source. Prevents reprocessing without data loss.

---

## Pipeline 4 — EDMS Patient ETL

**File:** `pipelines/edms_patient_etl.py`

Full ETL pipeline for patient records from the SecureEDMS dental practice portal — which has no API, only a JavaScript-heavy web portal.

```
Selenium login → navigate to report → set date range in modal
  → trigger Excel export → wait for download → pandas clean
  → load to staging (public schema) → transfer to prod (crm_data) + name mapping
```

### Key engineering problems

**Modal date inputs not responding to Selenium:**
Standard `send_keys` wasn't registering. Fixed with a JS helper that fires all required events:
```python
def js_set(driver, el, value):
    driver.execute_script("""
        el.value = v;
        el.dispatchEvent(new Event('input', {bubbles:true}));
        el.dispatchEvent(new Event('change', {bubbles:true}));
        el.dispatchEvent(new Event('blur',  {bubbles:true}));
    """, el, value)
```

**Staging → production transfer:**
Data lands in `public.edms_patient_data` first, then a transfer query moves it to `crm_data.edms_patient_data` with clinic name mapping applied, then truncates staging.

---

## Clinic Name Mapping

**The core data quality problem:** the same physical clinic appears under different names across every source system.

| Source | Raw name | Mapped to |
|---|---|---|
| Google Ads | "67th Street Dental" | "67th Street Dental" |
| Facebook Ads | "67th St Dental - NYC" | "67th Street Dental" |
| EDMS | "67 Street Dental Care" | "67th Street Dental" |

**Solution:** `crm_data.fb_google_clinics_mapping` — a lookup table built manually for all 50+ clinics. Applied at load time in every pipeline:

```sql
SELECT
    COALESCE(m.new_name, s.clinic_name) AS clinic,
    s.*
FROM source_table s
LEFT JOIN crm_data.fb_google_clinics_mapping m
    ON m.old_name = s.clinic_name;
```

Without this, dashboards would show each clinic fragmented into multiple entries, making cross-source analysis impossible.

---

## Project structure

```
clinic-analytics-pipeline/
├── README.md
├── pipelines/
│   ├── incremental_ads_to_crm.py     # Fivetran → crm_data incremental sync
│   ├── akitu_patient_attrition.py    # Akitu API → daily patient counts
│   ├── azure_blob_etl.py             # Azure Blob CSV → crm_data + archive
│   └── edms_patient_etl.py           # SecureEDMS scrape → staging → prod
└── staging/                          # Temp download dir (EDMS Excel files)
```

---

## Setup

```bash
pip install psycopg2-binary pandas selenium requests \
            azure-storage-blob openpyxl webdriver-manager
```

All `DB_CONFIG`, API credentials, and connection strings have been removed. Set these as environment variables or a config file before running.

---

## Notes

- Built for a commercial dental chain with 50+ clinic locations across the US.
- All credentials, connection strings, and client-specific endpoints have been removed.
- Fivetran connectors were configured by me — Google Ads, Facebook Ads, and QuickBooks.
- The `fb_google_clinics_mapping` table was built and maintained manually as part of this project.
