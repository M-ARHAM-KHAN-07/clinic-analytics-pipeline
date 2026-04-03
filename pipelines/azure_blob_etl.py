import io
import csv
import logging
import psycopg2
import time
from psycopg2.extras import execute_values
from azure.storage.blob import BlobServiceClient

# --- 1. Configuration ---
# Ensure this Connection String has 'srt=sco' (Service, Container, Object)
AZURE_CONNECTION_STRING = ""
CONTAINER_NAME = ""
ARCHIVE_PREFIX = ""

DB_CONFIG = {
    "host": "",
    "database": "",
    "user": "",
    "password": "",
    "port": ""
}

TABLE_NAME = ""

# --- 2. Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_patient_etl():
    conn = None
    try:
        # Initialize Clients
        service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = service_client.get_container_client(CONTAINER_NAME)
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        logger.info(f"Connected to container: {CONTAINER_NAME}")
        blobs = container_client.list_blobs()
        
        for blob in blobs:
            # Skip subfolders and archived files
            if blob.name.lower().startswith(ARCHIVE_PREFIX.lower()) or "/" in blob.name or not blob.name.endswith('.csv'):
                continue

            logger.info(f"--- Processing File: {blob.name} ---")
            blob_client = container_client.get_blob_client(blob.name)
            content = blob_client.download_blob().content_as_text()
            
            f = io.StringIO(content)
            reader = csv.reader(f)
            
            try:
                # 1. Map Headers
                header = [h.strip().lower() for h in next(reader)]
                
                def find_col(possible_names):
                    for name in possible_names:
                        for i, h in enumerate(header):
                            if name in h: return i
                    return None

                idx = {
                    "clinic": find_col(["clinic", "department", "office"]),
                    "annual": find_col(["annual", "year_target"]),
                    "monthly": find_col(["monthly", "month_target"]),
                    "weekly": find_col(["weekly", "week_target"])
                }

                # Filter out None values to find the max index needed for reading
                valid_indices = [v for v in idx.values() if v is not None]
                
                if idx["clinic"] is None or not valid_indices:
                    logger.error(f"Skipping {blob.name}. Could not map columns. Found: {header}")
                    continue

                # 2. Process Rows
                rows = []
                for row in reader:
                    if row and len(row) > max(valid_indices):
                        try:
                            def to_int(val):
                                if not val or val.strip() == "": return 0
                                # Handles decimals, $, and commas before converting to int
                                return int(float(val.replace('$', '').replace(',', '').strip()))

                            clinic = row[idx["clinic"]].strip()
                            annual = to_int(row[idx["annual"]]) if idx["annual"] is not None else 0
                            monthly = to_int(row[idx["monthly"]]) if idx["monthly"] is not None else 0
                            weekly = to_int(row[idx["weekly"]]) if idx["weekly"] is not None else 0
                            
                            rows.append((clinic, annual, monthly, weekly))
                        except Exception as row_e:
                            logger.warning(f"Row error in {blob.name}: {row_e}")

                # 3. Database Insert
                if rows:
                    insert_sql = f"INSERT INTO {TABLE_NAME} (clinic, annual_target, monthly_target, weekly_target) VALUES %s"
                    execute_values(cur, insert_sql, rows)
                    logger.info(f"Successfully loaded {len(rows)} rows into {TABLE_NAME}")

                    # 4. Archive Logic
                    archive_path = f"{ARCHIVE_PREFIX}{blob.name}"
                    archive_blob_client = container_client.get_blob_client(archive_path)
                    
                    # Use blob_client.url (it contains the necessary SAS token)
                    archive_blob_client.start_copy_from_url(blob_client.url)
                    
                    # Verify copy completion before deleting source
                    for _ in range(5):
                        props = archive_blob_client.get_blob_properties()
                        if props.copy.status == 'success':
                            blob_client.delete_blob()
                            logger.info(f"Archived {blob.name} to {archive_path}")
                            break
                        time.sleep(1)

            except Exception as e:
                logger.error(f"Failed to process {blob.name}: {e}")

        conn.commit()
    except Exception as e:
        logger.error(f"Critical Job Failure: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    run_patient_etl()
