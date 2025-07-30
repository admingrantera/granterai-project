# parse_charities.py (FINAL CORRECTED VERSION - 10,000 FILE TEST)

from dotenv import load_dotenv
load_dotenv()

import os
import xml.etree.ElementTree as ET
from multiprocessing import Pool, cpu_count
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch
from tqdm import tqdm
import re

# --- CONFIGURATION ---
FILE_LIST_PATH = "charity_file_list.txt"
NUM_PROCESSES = 4
db_pool = None

def init_worker(db_dsn):
    """Initializes a database connection pool for each worker process."""
    global db_pool
    db_pool = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=2, dsn=db_dsn)

def convert_windows_path_to_wsl(path):
    """Converts a Windows path to a format Ubuntu can use."""
    path = path.strip()
    if path.startswith('C:'):
        return '/mnt/c' + path[2:].replace('\\', '/')
    return path

def parse_charity_data(filepath):
    """
    Parses a single Form 990 XML file to extract rich charity data.
    """
    try:
        # Use a regex to strip namespaces, which is more robust for these files
        with open(filepath, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        # This regex removes both the xmlns attribute and the nsX: prefixes
        xml_content = re.sub(r'\sxmlns(:\w+)?="[^"]+"', '', xml_content)
        xml_content = re.sub(r'<\w+:', '<', xml_content)
        xml_content = re.sub(r'</\w+:', '</', xml_content)
        
        root = ET.fromstring(xml_content)

        def find_text(path, parent=root):
            element = parent.find(path)
            return element.text.strip() if element is not None and element.text else None

        ein = find_text('.//Filer/EIN')
        if not ein: return None

        charity_profile = {
            'ein': ein,
            'mission_statement': find_text('.//MissionDesc') or find_text('.//ActivityOrMissionDesc'),
            'address_line_1': find_text('.//Filer/USAddress/AddressLine1Txt'),
            'zip_code': find_text('.//Filer/USAddress/ZIPCd')
        }
        
        financials = []
        tax_year = find_text('.//TaxYr')
        cy_revenue = find_text('.//CYTotalRevenueAmt')
        cy_expenses = find_text('.//CYTotalExpensesAmt')
        
        if tax_year and (cy_revenue or cy_expenses):
            try:
                financials.append({
                    'ein': ein, 
                    'tax_year': int(tax_year),
                    'total_revenue': int(cy_revenue) if cy_revenue else None,
                    'total_expenses': int(cy_expenses) if cy_expenses else None
                })
            except (ValueError, TypeError):
                pass # Ignore if financial data is not a valid number
            
        return (charity_profile, financials)
            
    except (ET.ParseError, FileNotFoundError, AttributeError):
        return None

def main():
    print("--- Starting Final Enhanced Public Charity Parser (10,000 FILE TEST) ---")
    
    db_dsn = os.environ.get("DATABASE_URL")
    if not db_dsn:
        raise ValueError("DATABASE_URL not found in .env file.")

    try:
        with open(FILE_LIST_PATH, 'r') as f:
            all_files = [convert_windows_path_to_wsl(line) for line in f]
    except FileNotFoundError:
        print(f"ERROR: The file list '{FILE_LIST_PATH}' was not found.")
        return

    if not all_files:
        print("The file list is empty. No files to process.")
        return
            
    # --- TEST RUN ON 10,000 FILES ---
    files_to_process = all_files[:10000]
    print(f"Found {len(all_files)} total files. Processing a sample of {len(files_to_process)} for this test...")

    with Pool(processes=NUM_PROCESSES, initializer=init_worker, initargs=(db_dsn,)) as pool:
        results = list(tqdm(pool.imap_unordered(parse_charity_data, files_to_process), total=len(files_to_process), desc="Parsing Charity Data"))

    profile_updates = [res[0] for res in results if res and res[0] and res[0]['mission_statement']]
    financial_records = [fin for res in results if res for fin in res[1]]

    conn = psycopg2.connect(db_dsn)
    try:
        with conn.cursor() as cursor:
            if profile_updates:
                print(f"\nFound {len(profile_updates)} charities with mission statements. Updating main profiles...")
                execute_batch(cursor, 
                    "UPDATE charities SET mission_statement = %(mission_statement)s, address_line_1 = %(address_line_1)s, zip_code = %(zip_code)s WHERE ein = %(ein)s",
                    profile_updates)
                conn.commit()
                print("Charity profiles updated.")

            if financial_records:
                print(f"\nFound {len(financial_records)} annual financial records. Inserting...")
                # Clean the financials table before inserting new test data
                cursor.execute("TRUNCATE charity_financials RESTART IDENTITY;")
                execute_batch(cursor,
                    """
                    INSERT INTO charity_financials (ein, tax_year, total_revenue, total_expenses)
                    VALUES (%(ein)s, %(tax_year)s, %(total_revenue)s, %(total_expenses)s)
                    ON CONFLICT (ein, tax_year) DO NOTHING;
                    """,
                    financial_records)
                conn.commit()
                print("Financial records inserted.")
                
        print("\n--- Test Charity Data Import Complete ---")

    except Exception as e:
        print(f"Database update failed: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()

