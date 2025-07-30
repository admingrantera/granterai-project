# populate_foundations.py

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

def parse_foundation_data(filepath):
    """
    Parses a single XML file and extracts ONLY the foundation's core data.
    Returns a dictionary of the foundation data on success, None on failure.
    """
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        ns = {'irs': 'http://www.irs.gov/efile'}

        def find_text(path, parent=root):
            element = parent.find(path, ns)
            return element.text.strip() if element is not None and element.text else None

        ein = find_text('.//irs:Filer/irs:EIN')
        if not ein:
            return None

        # Extract data, providing None as a default for missing fields
        foundation_data = {
            'ein': ein,
            'name': find_text('.//irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt'),
            'address_line_1': find_text('.//irs:Filer/irs:USAddress/irs:AddressLine1Txt'),
            'city': find_text('.//irs:Filer/irs:USAddress/irs:CityNm'),
            'state': find_text('.//irs:Filer/irs:USAddress/irs:StateAbbreviationCd'),
            'zip_code': find_text('.//irs:Filer/irs:USAddress/irs:ZIPCd'),
            'mission_statement': find_text('.//irs:ActivityOrMissionDesc') or find_text('.//irs:MissionDesc')
        }

        # Only return data if we have the essentials: EIN and Name
        if foundation_data['ein'] and foundation_data['name']:
            return foundation_data
        else:
            return None

    except Exception:
        return None

if __name__ == '__main__':
    print("--- Starting Foundation Data Population ---")

    db_dsn = os.environ.get("DATABASE_URL")
    if not db_dsn:
        raise ValueError("DATABASE_URL not found in .env file.")

    # Truncate the foundations table to ensure a clean import
    conn = psycopg2.connect(db_dsn)
    with conn.cursor() as cursor:
        print("Cleaning the 'foundations' table before import...")
        cursor.execute("TRUNCATE foundations RESTART IDENTITY CASCADE;") # Use CASCADE to handle dependencies
        conn.commit()
    conn.close()
    print("Table cleaned.")

    FILE_LIST_PATH = "file_list.txt"
    try:
        with open(FILE_LIST_PATH, 'r') as f:
            files_to_process = [line.strip() for line in f]
    except FileNotFoundError:
        print(f"ERROR: The file list '{FILE_LIST_PATH}' was not found.")
        exit()

    if files_to_process:
        print(f"Found {len(files_to_process)} files to parse for foundation data...")

        with Pool(processes=NUM_PROCESSES, initializer=init_worker, initargs=(db_dsn,)) as pool:
            results = list(tqdm(pool.imap_unordered(parse_foundation_data, files_to_process), total=len(files_to_process), desc="Parsing Foundations"))

        # Filter out any None results from failed parses
        foundations_to_insert = [res for res in results if res is not None]

        if foundations_to_insert:
            print(f"Found {len(foundations_to_insert)} unique foundations. Inserting into database...")
            conn = psycopg2.connect(db_dsn)
            try:
                with conn.cursor() as cursor:
                    execute_batch(cursor,
                      """
                      INSERT INTO foundations (ein, name, address_line_1, city, state, zip_code, mission_statement)
                      VALUES (%(ein)s, %(name)s, %(address_line_1)s, %(city)s, %(state)s, %(zip_code)s, %(mission_statement)s)
                      ON CONFLICT (ein) DO NOTHING;
                      """,
                      foundations_to_insert
                    )
                    conn.commit()
                print("Successfully inserted foundation data.")
            except Exception as e:
                print(f"Database insertion failed: {e}")
            finally:
                if conn:
                    conn.close()
        else:
            print("No valid foundation data was found in the sample files.")

    print("\n--- Foundation data population complete. ---")
