# diagnose_enrichment_failure.py

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
        
        print("--- Diagnosing Enrichment Failure ---")

        with conn.cursor() as cursor:
            # 1. Get a sample of grants that SHOULD have matched in Tier 1
            print("Fetching a sample of 10 grants that have a recipient EIN...")
            cursor.execute("""
                SELECT id, recipient_name, recipient_ein 
                FROM grants 
                WHERE recipient_ein IS NOT NULL 
                LIMIT 10;
            """)
            sample_grants = cursor.fetchall()

            if not sample_grants:
                print("Diagnostic failed: No grants with a recipient_ein were found in the database.")
                return

            print("\n--- Checking for these EINs in the 'charities' table ---")
            
            found_count = 0
            for grant in sample_grants:
                recipient_ein = grant['recipient_ein']
                print(f"\nChecking for: '{grant['recipient_name']}' (EIN: {recipient_ein})")
                
                cursor.execute("SELECT name, ein FROM charities WHERE ein = %s;", (recipient_ein,))
                match = cursor.fetchone()
                
                if match:
                    print(f"  [SUCCESS] Found a match in charities table: '{match['name']}'")
                    found_count += 1
                else:
                    print(f"  [FAILURE] This EIN was NOT found in the charities table.")
            
            print("\n--- Diagnostic Complete ---")
            if found_count == 0:
                print("Result: 100% of the sample EINs were missing from the charities table.")
                print("This confirms the issue is with the master_charities.csv data source.")
            else:
                print(f"Result: {found_count}/10 sample EINs were found. The issue may be more complex.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
