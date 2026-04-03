import logging
import psycopg2
from psycopg2.extras import DictCursor


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("incremental_ads_to_crm.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("ads_incremental")


DB_CONFIG = {
    "host": "",
    "port": ,
    "dbname": "",
    "user": "",
    "password": "",
}


# Table mapping:
# Each entry: source_schema + table → target_schema + table
TABLES = [
    # ---------- GOOGLE ADS ----------
    
    {
        "source_schema": "",
        "source_table": "",
        "target_schema": "",
        "target_table": "",
        "conflict_cols": ["", ""],
        "watermark_col": "",
     },
     {
         "source_schema": "",
         "source_table": "",
         "target_schema": "",
         "target_table": "",
         "conflict_cols": ["", "", ""],
         "watermark_col": "",
     },

    # ---------- FACEBOOK (GROUP 1) ----------
    {
       "source_schema": "",
       "source_table": "",
       "target_schema": "",
       "target_table": "",
       "conflict_cols": [""],
       "watermark_col": "",
    },
     {
        "source_schema": "",
        "source_table": "",
        "target_schema": "",
        "target_table": "",
        "conflict_cols": ["", "", ""],
        "watermark_col": "",
     },
]



def get_connection():
    logger.info(
        "Connecting to PostgreSQL %s:%s / db=%s",
        DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"]
    )
    conn = psycopg2.connect(**DB_CONFIG)
    logger.info("PostgreSQL connection established.")
    return conn


def table_exists(cur, schema_name: str, table_name: str) -> bool:
    cur.execute("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name   = %s;
    """, (schema_name, table_name))
    return cur.fetchone() is not None


def show_table_info(cur, schema_name: str, table_name: str, sample_limit: int = 3):
    full_name = f"{schema_name}.{table_name}"
    cur.execute(f"SELECT COUNT(*) AS cnt FROM {schema_name}.{table_name};")
    cnt = cur.fetchone()["cnt"]
    logger.info("Table %s row count: %s", full_name, cnt)

    if cnt > 0:
        cur.execute(
            f"SELECT * FROM {schema_name}.{table_name} LIMIT %s;",
            (sample_limit,)
        )
        rows = cur.fetchall()
        for i, r in enumerate(rows, start=1):
            logger.info("Sample row %d from %s: %s", i, full_name, dict(r))
    else:
        logger.info("Table %s is empty.", full_name)


def get_columns(cur, schema_name: str, table_name: str):
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name   = %s
        ORDER BY ordinal_position;
    """, (schema_name, table_name))
    cols = [r["column_name"] for r in cur.fetchall()]
    if not cols:
        raise RuntimeError(f"No columns found for {schema_name}.{table_name}")
    return cols


def get_max_watermark(cur, schema_name: str, table_name: str, wm_col: str):
    query = f"SELECT MAX({wm_col}) AS max_wm FROM {schema_name}.{table_name};"
    logger.debug("Watermark query: %s", query)
    cur.execute(query)
    row = cur.fetchone()
    max_wm = row["max_wm"] if row and row["max_wm"] else None
    logger.info("Current watermark %s for %s.%s = %s", wm_col, schema_name, table_name, max_wm)
    return max_wm



def run_incremental_upsert(
    cur,
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
    conflict_cols,
    wm_col: str,
):
    logger.info(
        "Starting incremental upsert %s.%s → %s.%s (conflict_cols=%s, wm_col=%s)",
        source_schema, source_table, target_schema, target_table, conflict_cols, wm_col
    )

    # 1) Get current watermark from target
    last_wm = get_max_watermark(cur, target_schema, target_table, wm_col)

    # 2) Get ordered columns from target
    cols = get_columns(cur, target_schema, target_table)

    # 2a) Build SELECT column expressions + optional JOIN for name mapping
    # By default: s.col
    mapping_join = ""
    select_exprs = []

    if target_schema == "crm_data" and target_table in ("google_account_history"):
        # Map google_ads.account_history.descriptive_name
        for c in cols:
            if c == "descriptive_name":
                # Use mapped name if exists, else original descriptive_name
                select_exprs.append(
                    "COALESCE(m.new_name, s.descriptive_name) AS descriptive_name"
                )
            else:
                select_exprs.append(f"s.{c}")
        mapping_join = " LEFT JOIN crm_data.fb_google_clinics_mapping m ON m.old_name = s.descriptive_name"

    elif target_schema == "crm_data" and target_table in ("fb_account_history"):
        # Map facebook_group_1.account_history.name
        for c in cols:
            if c == "name":
                select_exprs.append(
                    "COALESCE(m.new_name, s.name) AS name"
                )
            else:
                select_exprs.append(f"s.{c}")
        mapping_join = " LEFT JOIN crm_data.fb_google_clinics_mapping m ON m.old_name = s.name"

    else:
        # For all other tables (stats, basic_ad, etc.) just pass through
        select_exprs = [f"s.{c}" for c in cols]

    col_list = ", ".join(cols)
    col_list_src = ", ".join(select_exprs)

    # 3) Build update set (exclude conflict_cols)
    update_cols = [c for c in cols if c not in conflict_cols]
    set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    # 4) SELECT from source with optional watermark filter
    select_query = f"SELECT {col_list_src} FROM {source_schema}.{source_table} s{mapping_join}"
    params = []
    if last_wm:
        select_query += f" WHERE s.{wm_col} > %s"
        params.append(last_wm)
        logger.info(
            "Incremental mode: selecting rows where %s > %s from %s.%s",
            wm_col, last_wm, source_schema, source_table
        )
    else:
        logger.info(
            "Full load mode for %s.%s (no watermark yet in target).",
            target_schema, target_table
        )

    conflict_cols_sql = ", ".join(conflict_cols)

    final_query = f"""
        INSERT INTO {target_schema}.{target_table} ({col_list})
        {select_query}
        ON CONFLICT ({conflict_cols_sql})
        DO NOTHING;
    """

    logger.debug("Final upsert SQL for %s.%s:\n%s", target_schema, target_table, final_query)
    logger.debug("Params: %s", params)

    cur.execute(final_query, params)
    affected = cur.rowcount
    logger.info(
        "Upsert done for %s.%s → %s.%s | rows inserted: %d",
        source_schema, source_table, target_schema, target_table, affected
    )
    return affected



def test_connection_and_tables() -> bool:
    logger.info("Running DB connection and table existence tests...")
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                logger.info("PostgreSQL version: %s", version)

                all_ok = True

                for t in TABLES:
                    src_schema = t["source_schema"]
                    src_table = t["source_table"]
                    tgt_schema = t["target_schema"]
                    tgt_table = t["target_table"]

                    logger.info(
                        "Checking pair: %s.%s → %s.%s",
                        src_schema, src_table, tgt_schema, tgt_table
                    )

                    # Source
                    if table_exists(cur, src_schema, src_table):
                        logger.info("Source table exists: %s.%s", src_schema, src_table)
                        show_table_info(cur, src_schema, src_table)
                    else:
                        logger.error("Source table MISSING: %s.%s", src_schema, src_table)
                        all_ok = False

                    # Target
                    if table_exists(cur, tgt_schema, tgt_table):
                        logger.info("Target table exists: %s.%s", tgt_schema, tgt_table)
                        show_table_info(cur, tgt_schema, tgt_table)
                    else:
                        logger.error("Target table MISSING: %s.%s", tgt_schema, tgt_table)
                        all_ok = False

                if all_ok:
                    logger.info("DB connection & table checks PASSED.")
                else:
                    logger.error("DB connection & table checks FAILED. Fix issues before running incremental load.")

                return all_ok

    except Exception:
        logger.exception("Error during DB connection / table checks.")
        return False



def run_incremental_load():
    logger.info("========== Starting incremental load ==========")
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                for t in TABLES:
                    run_incremental_upsert(
                        cur,
                        t["source_schema"],
                        t["source_table"],
                        t["target_schema"],
                        t["target_table"],
                        conflict_cols=t["conflict_cols"],
                        wm_col=t["watermark_col"],
                    )

            conn.commit()
            logger.info("✅ Incremental load completed and committed.")

    except Exception:
        logger.exception("❌ Incremental load failed; transaction may have been rolled back.")


if __name__ == "__main__":
    ok = test_connection_and_tables()
    if ok:
        run_incremental_load()
    else:
        logger.error("Skipping incremental load because pre-checks failed.")
