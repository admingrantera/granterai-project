# local_parser.py (UPGRADED to capture Recipient EIN)

from dotenv import load_dotenv
load_dotenv()

import os
import xml.etree.ElementTree as ET
from multiprocessing import Pool, cpu_count
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch
from tqdm import tqdm

# --- CONFIGURATION ---
NUM_PROCESSES = 6
db_pool = None

def init_worker(db_dsn):
    """Initializes a database connection pool for each worker process."""
    global db_pool
    db_pool = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=2, dsn=db_dsn)

def parse_and_save_data(filepath):
    """
    Parses a single 990 or 990-PF file and saves grant and officer data.
    """
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        ns = {'irs': 'http://www.irs.gov/efile'}

        def find_text(path, parent=root):
            element = parent.find(path, ns)
            return element.text.strip() if element is not None and element.text else None

        ein = find_text('.//irs:Filer/irs:EIN')
        if not ein: return False

        tax_year = find_text('.//irs:TaxYr')
        grants_data = []

        # 1. Check for 990-PF grant format
        pf_grant_elements = root.findall('.//irs:GrantOrContributionPdDurYrGrp', ns)
        for grant in pf_grant_elements:
            recipient_name = find_text('.//irs:RecipientBusinessName/irs:BusinessNameLine1Txt', parent=grant)
            grant_amount_str = find_text('.//irs:Amt', parent=grant)
            grant_purpose = find_text('.//irs:GrantOrContributionPurposeTxt', parent=grant)
            if recipient_name and grant_amount_str:
                try:
                    grants_data.append({
                        'foundation_ein': ein, 'tax_year': int(tax_year) if tax_year else None,
                        'recipient_name': recipient_name, 'grant_amount': int(grant_amount_str),
                        'grant_purpose': grant_purpose, 'recipient_ein': None # PF forms do not have recipient EIN
                    })
                except (ValueError, TypeError): continue

        # 2. Check for 990 Schedule I grant format
        schedule_i_elements = root.findall('.//irs:IRS990ScheduleI', ns)
        for schedule in schedule_i_elements:
            for grant in schedule.findall('.//irs:RecipientTable', ns):
                recipient_name = find_text('.//irs:RecipientBusinessName/irs:BusinessNameLine1Txt', parent=grant)
                grant_amount_str = find_text('.//irs:CashGrantAmt', parent=grant)
                grant_purpose = find_text('.//irs:PurposeOfGrantTxt', parent=grant)
                # --- THIS IS THE KEY UPGRADE ---
                recipient_ein = find_text('.//irs:RecipientEIN', parent=grant)
                if recipient_name and grant_amount_str:
                    try:
                        grants_data.append({
                            'foundation_ein': ein, 'tax_year': int(tax_year) if tax_year else None,
                            'recipient_name': recipient_name, 'grant_amount': int(grant_amount_str),
                            'grant_purpose': grant_purpose, 'recipient_ein': recipient_ein
                        })
                    except (ValueError, TypeError): continue

        if not grants_data: return True

        conn = db_pool.getconn()
        try:
            with conn.cursor() as cursor:
                execute_batch(cursor, """
                    INSERT INTO grants (foundation_ein, tax_year, recipient_name, grant_amount, grant_purpose, recipient_ein)
                    VALUES (%(foundation_ein)s, %(tax_year)s, %(recipient_name)s, %(grant_amount)s, %(grant_purpose)s, %(recipient_ein)s)
                    ON CONFLICT DO NOTHING;
                """, grants_data)
            conn.commit()
        except Exception:
            conn.rollback()
            return False
        finally:
            db_pool.putconn(conn)
        return True
    except Exception:
        return False

if __name__ == '__main__':
    print("--- Starting Upgraded Universal Grants Parser (with EIN capture) ---")
    db_dsn = os.environ.get("DATABASE_URL")
    if not db_dsn: raise ValueError("DATABASE_URL not found.")

    conn = psycopg2.connect(db_dsn)
    with conn.cursor() as cursor:
        print("Cleaning grants table before import...")
        cursor.execute("TRUNCATE grants RESTART IDENTITY;")
        conn.commit()
    conn.close()
    print("Grants table cleaned.")

    FILE_LIST_PATH = "file_list.txt"
    try:
        with open(FILE_LIST_PATH, 'r') as f:
            files_to_process = [line.strip() for line in f]
    except FileNotFoundError:
        print(f"ERROR: '{FILE_LIST_PATH}' not found.")
        exit()

    if files_to_process:
        print(f"Found {len(files_to_process)} files to process...")
        with Pool(processes=NUM_PROCESSES, initializer=init_worker, initargs=(db_dsn,)) as pool:
            results = list(tqdm(pool.imap_unordered(parse_and_save_data, files_to_process), total=len(files_to_process), desc="Parsing Grants"))
            success_count = sum(1 for r in results if r)
    
    print(f"\n--- Import complete ---")
    print(f"Successfully processed: {success_count}/{len(files_to_process)}")
    print(f"Failed to process: {len(files_to_process) - success_count}/{len(files_to_process)}")
