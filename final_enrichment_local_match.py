# final_enrichment_local_match.py

from dotenv import load_dotenv
load_dotenv()

import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from fuzzywuzzy import process
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from collections import defaultdict

# --- CONFIGURATION ---
NUM_PROCESSES = 6
SCORE_CUTOFF = 95 

# --- GLOBAL DICTIONARY FOR WORKERS ---
charity_data_by_state_global = None

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

def init_worker(charity_data_by_state):
    """Initializes the geographically sorted data for each worker process."""
    global charity_data_by_state_global
    charity_data_by_state_global = charity_data_by_state

def match_grant_recipient_local(grant):
    """The core, locally-run matching logic."""
    grant_id = grant['id']
    foundation_state = grant['state']
    recipient_name = grant['recipient_name']
    
    if not recipient_name or not foundation_state:
        return None

    state_specific_charities = charity_data_by_state_global.get(foundation_state)
    if not state_specific_charities:
        return None

    normalized_recipient = normalize_name(recipient_name)
    choices = list(state_specific_charities.keys())
    
    best_match = process.extractOne(normalized_recipient, choices, score_cutoff=SCORE_CUTOFF)
    
    if best_match:
        matched_normalized_name = best_match[0]
        matched_ein = state_specific_charities.get(matched_normalized_name)
        if matched_ein:
            return (matched_ein, grant_id)
    
    return None

def main():
    conn = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        
        print("--- Starting Final Grant Enrichment (Local Parallel Processing) ---")
        
        # Phase 1: Fetch all data in a single connection
        print("Building state-based charity lookup maps...")
        charity_data_by_state = defaultdict(dict)
        with conn.cursor() as cursor:
            cursor.execute("SELECT ein, name, state FROM charities WHERE name IS NOT NULL AND state IS NOT NULL")
            for row in tqdm(cursor.fetchall(), desc="Organizing Charities by State"):
                normalized = normalize_name(row['name'])
                state = row['state']
                if normalized:
                    charity_data_by_state[state][normalized] = row['ein']
        
        print(f"Lookup maps built for {len(charity_data_by_state)} states.")

        with conn.cursor() as cursor:
            print("Fetching unmatched grants with foundation location...")
            cursor.execute("""
                SELECT g.id, g.recipient_name, f.state 
                FROM grants g
                JOIN foundations f ON g.foundation_ein = f.ein
                WHERE g.recipient_ein_matched IS NULL AND g.recipient_name IS NOT NULL AND f.state IS NOT NULL
            """)
            grants_to_enrich = cursor.fetchall()

        if not grants_to_enrich:
            print("No processable unmatched grants found.")
            return

        # Phase 2: Process the data locally in parallel
        print(f"Found {len(grants_to_enrich)} grants to enrich. Starting local parallel processing...")
        with Pool(processes=NUM_PROCESSES, initializer=init_worker, initargs=(charity_data_by_state,)) as pool:
            results = list(tqdm(pool.imap_unordered(match_grant_recipient_local, grants_to_enrich), total=len(grants_to_enrich), desc="Enriching Grants"))

        updates_to_make = [res for res in results if res is not None]

        # Phase 3: Update the database in a single batch
        if updates_to_make:
            print(f"\nFound {len(updates_to_make)} new high-confidence matches. Updating database...")
            with conn.cursor() as cursor:
                execute_batch(cursor, "UPDATE grants SET recipient_ein_matched = %s WHERE id = %s", updates_to_make)
                conn.commit()
            print(f"Successfully updated {cursor.rowcount} grant records.")
        else:
            print("\nNo new high-confidence matches were found in this run.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
