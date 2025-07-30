# final_match_and_update.py

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from tqdm import tqdm

def main():
    conn = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL not found.")
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        
        print("--- Starting Definitive Grant Enrichment ---")

        with conn.cursor() as cursor:
            # Step 1: Fetch all charity EINs into a fast Python set
            print("Fetching all charity EINs into memory...")
            cursor.execute("SELECT ein FROM charities;")
            # This TRIMs any whitespace during the fetch
            charity_eins = {row['ein'].strip() for row in cursor.fetchall() if row['ein']}
            print(f"Loaded {len(charity_eins)} unique charity EINs.")

            # Step 2: Fetch all unmatched grants that have a recipient EIN
            print("Fetching all processable unmatched grants...")
            cursor.execute("""
                SELECT id, recipient_ein 
                FROM grants 
                WHERE recipient_ein IS NOT NULL AND recipient_ein_matched IS NULL;
            """)
            unmatched_grants = cursor.fetchall()

        if not unmatched_grants:
            print("No unmatched grants with recipient EINs were found.")
            return

        print(f"Found {len(unmatched_grants)} grants to check. Performing match in memory...")
        
        # Step 3: Find the intersection in Python
        updates_to_make = []
        for grant in tqdm(unmatched_grants, desc="Matching Grants"):
            # We TRIM the grant's recipient_ein here to match the clean set
            if grant['recipient_ein'] and grant['recipient_ein'].strip() in charity_eins:
                updates_to_make.append((grant['recipient_ein'].strip(), grant['id']))

        # Step 4: Perform the final, targeted update
        if updates_to_make:
            print(f"\nFound {len(updates_to_make)} direct EIN matches. Updating database...")
            with conn.cursor() as cursor:
                execute_batch(cursor, "UPDATE grants SET recipient_ein_matched = %s WHERE id = %s", updates_to_make)
                conn.commit()
            print(f"Successfully updated {len(updates_to_make)} grant records.")
        else:
            print("\nNo direct EIN matches were found in this run.")

        # Final verification
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM grants WHERE recipient_ein_matched IS NULL;")
            remaining_unmatched = cursor.fetchone()['count']
            print(f"Remaining unmatched grants: {remaining_unmatched}")


    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
