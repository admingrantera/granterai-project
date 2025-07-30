# load_master_charities.py (FINAL CORRECTED VERSION)

from dotenv import load_dotenv
load_dotenv()

import os
import csv
import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm

# --- CONFIGURATION ---
MASTER_CHARITIES_CSV = "master_charities.csv"
BATCH_SIZE = 5000

# --- DATABASE CONNECTION ---
def get_db_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL not found in .env file.")
    return psycopg2.connect(db_url)

def main():
    """
    Reads the master charity CSV and performs a bulk insert into the 'charities' table.
    """
    print("--- Starting Master Charity List Import ---")
    
    if not os.path.exists(MASTER_CHARITIES_CSV):
        print(f"ERROR: The master file '{MASTER_CHARITIES_CSV}' was not found.")
        return

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("Cleaning the 'charities' table before import...")
        cursor.execute("TRUNCATE charities RESTART IDENTITY;")
        conn.commit()

        print(f"Reading '{MASTER_CHARITIES_CSV}' and preparing data for insert...")
        
        charities_to_insert = []
        with open(MASTER_CHARITIES_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            headers = [h.lower() for h in reader.fieldnames]
            reader.fieldnames = headers
            
            print(f"Detected and standardized CSV Headers: {headers}")

            # --- THIS IS THE CORRECTED PART ---
            # The script now only requires the headers that are actually in your file.
            required_headers = ['ein', 'name', 'city', 'state']
            if not all(h in headers for h in required_headers):
                print("\n--- ERROR ---")
                print(f"The CSV file is missing one or more required headers. Expected: {required_headers}")
                print(f"Found: {headers}")
                return

            for i, row in enumerate(reader):
                try:
                    # We only map the columns that exist in the CSV.
                    # The database table allows address_line_1 and zip_code to be empty (NULL).
                    charities_to_insert.append({
                        'ein': row['ein'],
                        'name': row['name'],
                        'city': row['city'],
                        'state': row['state'],
                        'address_line_1': None, # Set to None as it's not in the CSV
                        'zip_code': None       # Set to None as it's not in the CSV
                    })
                except KeyError as e:
                    print(f"\n--- ERROR ---")
                    print(f"A required column is missing from the CSV file: {e}")
                    print(f"This error occurred on row number: {i + 2}")
                    print(f"The available columns in this row are: {list(row.keys())}")
                    return

        if not charities_to_insert:
            print("No charities found in the CSV file.")
            return

        print(f"Found {len(charities_to_insert)} charities. Starting bulk insert...")

        with tqdm(total=len(charities_to_insert), desc="Inserting Charities") as pbar:
            execute_batch(
                cursor,
                """
                INSERT INTO charities (ein, name, city, state, address_line_1, zip_code)
                VALUES (%(ein)s, %(name)s, %(city)s, %(state)s, %(address_line_1)s, %(zip_code)s)
                ON CONFLICT (ein) DO NOTHING;
                """,
                charities_to_insert,
                page_size=BATCH_SIZE
            )
            pbar.update(len(charities_to_insert))
        
        conn.commit()
        print(f"\nSuccessfully inserted {cursor.rowcount if cursor.rowcount > 0 else len(charities_to_insert)} records into the 'charities' table.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            
    print("--- Master Charity List Import Complete ---")

if __name__ == "__main__":
    main()

