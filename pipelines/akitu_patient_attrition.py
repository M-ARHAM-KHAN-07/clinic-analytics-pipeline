import requests
import psycopg2
from datetime import datetime, timezone

# --- Database Config ---
DB_HOST = ""
DB_PORT = ""
DB_NAME = ""
DB_USER = ""
DB_PASS = ""

# --- API Config ---
BASE_URL = ""
LOGIN_ENDPOINT = ""
ATTRITION_ENDPOINT = ""
USERNAME = ""
PASSWORD = ""


def get_todays_data_from_api():
    """
    Logs into the API and fetches attrition data for the current day.
    Returns: tuple (date_obj, new_patients, lost_patients)
    """
    # Calculate Date Range (Today UTC)
    now = datetime.now(timezone.utc)
    start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    target_date = start_date.date()

    print(f" Target Date: {target_date}")

    # Session Setup
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": BASE_URL,
        "Referer": BASE_URL + "/"
    })

    # Login
    print(" Logging in to API...")
    try:
        login_resp = session.post(
            BASE_URL + LOGIN_ENDPOINT,
            json={"username": USERNAME, "password": PASSWORD},
            timeout=30
        )
        login_resp.raise_for_status()
    except Exception as e:
        raise Exception(f"API Login Failed: {e}")

    # Fetch Data
    print( Fetching attrition report...")
    payload = {
        "practiceDateTimeRange": {
            "startDateTime": start_date.isoformat().replace("+00:00", "Z"),
            "endDateTime": end_date.isoformat().replace("+00:00", "Z")
        },
        "includeNewPatients": True,
        "includeLostPatients": True,
        "includeIdlePatients": False, # Not needed for DB
        "specifiedDaysForIdlePatients": 90
    }

    try:
        resp = session.post(
            BASE_URL + ATTRITION_ENDPOINT,
            json=payload,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        
        new_pts = data.get("totalNewPatients", 0)
        lost_pts = data.get("totalLostPatients", 0)
        
        print(f" Data Retrieved: New={new_pts}, Lost={lost_pts}")
        return target_date, new_pts, lost_pts

    except Exception as e:
        raise Exception(f"API Fetch Failed: {e}")

def save_to_postgres(date_val, new_count, lost_count):
    """
    Inserts or updates the daily count in PostgreSQL.
    """
    conn = None
    try:
        print(" Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()

        # 1. Create table if not exists
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS crm_data.akitu_patient_data (
                date DATE PRIMARY KEY,
                new_patients INTEGER NOT NULL,
                lost_patients INTEGER NOT NULL
            );
        """
        cursor.execute(create_table_sql)

        # 2. Insert or Update (Upsert)
        # If the script runs multiple times in one day, it updates the existing row rather than failing.
        upsert_sql = """
            INSERT INTO crm_data.akitu_patient_data (date, new_patients, lost_patients)
            VALUES (%s, %s, %s)
            ON CONFLICT (date) DO UPDATE
            SET 
                new_patients = EXCLUDED.new_patients,
                lost_patients = EXCLUDED.lost_patients;
        """
        
        cursor.execute(upsert_sql, (date_val, new_count, lost_count))
        conn.commit()
        print("💾 Database successfully updated.")

    except Exception as e:
        print(f" Database Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    try:
        # Step 1: Get Data
        report_date, new, lost = get_todays_data_from_api()
        
        # Step 2: Save to DB
        save_to_postgres(report_date, new, lost)
        
    except Exception as error:
        print(f" Process failed: {error}")
