# precompute_normalized_names.py

from dotenv import load_dotenv
load_dotenv()

import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from tqdm import tqdm
from multiprocessing import Pool

# --- CONFIGURATION ---
NUM_PROCESSES = 6

def normalize_name(name):
    """A consistent function to clean and simplify organization names."""
    if not name: return ""
    name = name.upper().replace('&', 'AND')
    name = re.sub(r'[^\w\s]', '', name)
    suffixes_to_remove = [
        'INC', 'INCORPORATED', 'LLC', 'CORP', 'CORPORATION', 'FOUNDATION',
        'FDN', 'FUND', 'TRUST', 'CHARITABLE', 'CHARITY', 'ASSOCIATION',
        'THE', 'AND', 'OF', 'FOR'
    ]
    for suffix in suffixes_to_remove:
        name = re.sub(r'\b' + re.escape(suffix) + r'\b', '', name)
    return re.sub(r'\s+', ' ', name).strip()

def process_batch(batch):
    """Worker function to normalize a batch of names."""
    updates = []
    for record in batch:
        # Assumes record has an 'id' and a 'name' or 'recipient_name'
        record_id = record.get('id') or record.get('ein')
        name_to_process = record.get('name') or record.get('recipient_name')
        if record_id and name_to_process:
            updates.append((normalize_name(name_to_process), record_id))
    return updates

def main():
    conn = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        
        print("--- Starting Pre-computation of Normalized Names ---")

        # --- Process Charities Table ---
        with conn.cursor() as cursor:
            print("Fetching all charities...")
            cursor.execute("SELECT ein, name FROM charities WHERE name IS NOT NULL")
            all_charities = cursor.fetchall()

        if all_charities:
            print(f"Normalizing {len(all_charities)} charity names...")
            charity_batches = [all_charities[i:i + 10000] for i in range(0, len(all_charities), 10000)]
            with Pool(processes=NUM_PROCESSES) as pool:
                results = list(tqdm(pool.imap(process_batch, charity_batches), total=len(charity_batches), desc="Processing Charities"))
            
            charity_updates = [item for sublist in results for item in sublist]
            
            print("Updating charities table in the database...")
            with conn.cursor() as cursor:
                execute_batch(cursor, "UPDATE charities SET normalized_name = %s WHERE ein = %s", charity_updates)
                conn.commit()

        # --- Process Grants Table ---
        with conn.cursor() as cursor:
            print("\nFetching all grants...")
            cursor.execute("SELECT id, recipient_name FROM grants WHERE recipient_name IS NOT NULL")
            all_grants = cursor.fetchall()

        if all_grants:
            print(f"Normalizing {len(all_grants)} grant recipient names...")
            grant_batches = [all_grants[i:i + 10000] for i in range(0, len(all_grants), 10000)]
            with Pool(processes=NUM_PROCESSES) as pool:
                results = list(tqdm(pool.imap(process_batch, grant_batches), total=len(grant_batches), desc="Processing Grants"))

            grant_updates = [item for sublist in results for item in sublist]
            
            print("Updating grants table in the database...")
            with conn.cursor() as cursor:
                execute_batch(cursor, "UPDATE grants SET normalized_name = %s WHERE id = %s", grant_updates)
                conn.commit()

        print("\n--- Pre-computation complete. Database is now optimized for fast joins. ---")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
