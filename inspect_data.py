# inspect_data.py

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Using one of the EINs we know exists in both tables from our previous diagnostic
KNOWN_MATCH_EIN = '042105850' 

def main():
    conn = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL not found.")
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        
        print(f"--- Inspecting Raw Data for EIN: {KNOWN_MATCH_EIN} ---")

        with conn.cursor() as cursor:
            # Get the record from the grants table
            cursor.execute("SELECT recipient_ein FROM grants WHERE recipient_ein LIKE %s LIMIT 1;", (f'%{KNOWN_MATCH_EIN}%',))
            grant_record = cursor.fetchone()

            # Get the record from the charities table
            cursor.execute("SELECT ein FROM charities WHERE ein LIKE %s LIMIT 1;", (f'%{KNOWN_MATCH_EIN}%',))
            charity_record = cursor.fetchone()

            print("\n--- Raw Data Analysis ---")
            
            if grant_record:
                grant_ein = grant_record['recipient_ein']
                print(f"\nGrant Recipient EIN:")
                print(f"  - As String: '{grant_ein}'")
                print(f"  - Length: {len(grant_ein)}")
                print(f"  - As Bytes: {grant_ein.encode('utf-8')}")
            else:
                print("\n[FAILURE] Could not find the test EIN in the 'grants' table.")

            if charity_record:
                charity_ein = charity_record['ein']
                print(f"\nCharity EIN:")
                print(f"  - As String: '{charity_ein}'")
                print(f"  - Length: {len(charity_ein)}")
                print(f"  - As Bytes: {charity_ein.encode('utf-8')}")
            else:
                print("\n[FAILURE] Could not find the test EIN in the 'charities' table.")

            if grant_record and charity_record:
                if grant_ein == charity_ein:
                    print("\n[UNEXPECTED] The strings appear to be identical.")
                else:
                    print("\n[SUCCESS] The strings are different, confirming a data quality issue.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
