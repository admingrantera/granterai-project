# inspect_mismatched_grants.py

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor

def main():
    conn = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL not found.")
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        
        print("--- Running Final Data Integrity Diagnostic ---")

        with conn.cursor() as cursor:
            # Get a sample of grants that have a recipient EIN
            print("Fetching sample of grants where recipient_ein is populated...")
            cursor.execute("""
                SELECT id, recipient_ein, recipient_ein_matched 
                FROM grants 
                WHERE recipient_ein IS NOT NULL AND recipient_ein != ''
                LIMIT 10;
            """)
            sample_grants = cursor.fetchall()

            if not sample_grants:
                print("\n[CRITICAL FAILURE] No grants with a recipient_ein were found.")
                print("This indicates a problem with the last run of local_parser.py.")
                return

            print("\n--- Analyzing Sample Data ---")
            
            unmatched_but_populated = 0
            for grant in sample_grants:
                print(f"\nGrant ID: {grant['id']}")
                print(f"  - recipient_ein:          '{grant['recipient_ein']}'")
                print(f"  - recipient_ein_matched:  '{grant['recipient_ein_matched']}'")

                if grant['recipient_ein_matched'] is None:
                    print("  - Status: [CORRECT] Unmatched as expected.")
                else:
                    print("  - Status: [PROBLEM] This grant is already marked as 'matched'.")
                    unmatched_but_populated += 1
            
            print("\n--- Diagnostic Complete ---")
            if unmatched_but_populated > 0:
                print("Result: The recipient_ein_matched column was populated incorrectly during parsing.")
                print("We need to clean this column before enrichment can proceed.")
            else:
                 print("Result: The data appears correct. The enrichment script itself is flawed.")


    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
