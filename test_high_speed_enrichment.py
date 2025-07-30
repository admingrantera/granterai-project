# test_high_speed_enrichment.py

from dotenv import load_dotenv
load_dotenv()

import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from fuzzywuzzy import process
from tqdm import tqdm
from multiprocessing import Pool, Manager, cpu_count

# --- CONFIGURATION ---
NUM_PROCESSES = max(1, cpu_count() - 1)

# --- GLOBAL VARIABLES FOR WORKERS ---
normalized_ein_map_global = None
normalized_choices_global = None

def normalize_name(name):
    if not name: return ""
    name = name.upper().replace('&', 'AND')
    name = re.sub(r'[^\w\s]', '', name)
    suffixes_to_remove = [
        'INC', 'INCORPORATED', 'LLC', 'L L C', 'CORP', 'CORPORATION',
        'FOUNDATION', 'FDN', 'FUND', 'TRUST', 'CHARITABLE', 'CHARITY',
        'ASSOCIATION', 'SOCIETY', 'LEAGUE', 'CLUB', 'CENTER',
        'UNIVERSITY', 'UNIV', 'COLLEGE', 'SCHOOL', 'HOSPITAL',
        'THE', 'AND', 'OF', 'FOR', 'PROGRAM', 'DEPARTMENT'
    ]
    for suffix in suffixes_to_remove:
        name = re.sub(r'\b' + re.escape(suffix) + r'\b', '', name)
    return re.sub(r'\s+', ' ', name).strip()

def init_worker(ein_map, choices):
    """Initializes the global variables for each worker process."""
    global normalized_ein_map_global, normalized_choices_global
    normalized_ein_map_global = ein_map
    normalized_choices_global = choices

def match_grant_recipient(grant):
    """The core logic that each worker will run on a single grant."""
    grant_id = grant['id']
    normalized_recipient = normalize_name(grant['recipient_name'])
    if not normalized_recipient:
        return None

    direct_match_ein = normalized_ein_map_global.get(normalized_recipient)
    if direct_match_ein:
        return (direct_match_ein, grant_id)

    best_match = process.extractOne(normalized_recipient, normalized_choices_global, score_cutoff=95)
    if best_match:
        matched_normalized_name = best_match[0]
        fuzzy_match_ein = normalized_ein_map_global.get(matched_normalized_name)
        if fuzzy_match_ein:
            return (fuzzy_match_ein, grant_id)
            
    return None

def main():
    """Main function to orchestrate the enrichment process."""
    conn = None
    try:
        db_url = os.environ.get("DATABASE_URL") + "?sslmode=require"
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        
        # 1. Build the maps in the main process
        print("Building normalized foundation maps...")
        temp_map = {}
        with conn.cursor() as cursor:
            cursor.execute("SELECT ein, name FROM foundations WHERE name IS NOT NULL")
            for row in cursor.fetchall():
                normalized = normalize_name(row['name'])
                if normalized and normalized not in temp_map:
                    temp_map[normalized] = row['ein']
        
        normalized_ein_map = temp_map
        normalized_choices = list(normalized_ein_map.keys())
        print(f"Maps built with {len(normalized_choices)} unique normalized names.")

        # 2. Fetch a SAMPLE of 1,000 grants that need enrichment
        with conn.cursor() as cursor:
            # THIS IS THE ONLY CHANGE: WE ADD "LIMIT 1000" TO THE QUERY
            print("Fetching a sample of 1,000 grants for the test...")
            cursor.execute("SELECT id, recipient_name FROM grants WHERE recipient_ein_matched IS NULL AND recipient_name IS NOT NULL LIMIT 1000")
            grants_to_enrich = cursor.fetchall()

        if not grants_to_enrich:
            print("Could not find any grants to test. Ensure the database is populated.")
            return

        print(f"\nFound {len(grants_to_enrich)} grants to test. Starting parallel processing with {NUM_PROCESSES} cores...")

        # 3. Use a multiprocessing Pool to process the sample in parallel
        with Pool(processes=NUM_PROCESSES, initializer=init_worker, initargs=(normalized_ein_map, normalized_choices)) as pool:
            results = list(tqdm(pool.imap_unordered(match_grant_recipient, grants_to_enrich), total=len(grants_to_enrich), desc="Matching Sample"))

        updates_to_make = [res for res in results if res is not None]

        if updates_to_make:
            print(f"\nFound {len(updates_to_make)} matches in the sample. Updating database...")
            with conn.cursor() as cursor:
                execute_batch(cursor, "UPDATE grants SET recipient_ein_matched = %s WHERE id = %s", updates_to_make)
                conn.commit()
            print("Test enrichment complete.")
        else:
            print("\nNo new matches found in the sample.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
