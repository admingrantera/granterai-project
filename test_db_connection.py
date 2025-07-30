# test_db_connection.py

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor

def test_connection():
    """
    Connects to the database, wipes the grants table, inserts a single row,
    and prints the count to verify the connection.
    """
    print("--- Starting Database Connection Test ---")
    
    db_dsn = os.environ.get("DATABASE_URL")
    if not db_dsn:
        print("ERROR: DATABASE_URL not found in .env file.")
        return

    conn = None
    try:
        # 1. Connect to the database
        print(f"Attempting to connect to: {db_dsn.split('@')[1]}")
        conn = psycopg2.connect(db_dsn, cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        print("  - Success: Database connection established.")

        # 2. Clean the grants table
        print("Step 2: Wiping the 'grants' table...")
        cursor.execute("TRUNCATE grants RESTART IDENTITY;")
        conn.commit()
        print("  - Success: 'grants' table is now empty.")

        # 3. Insert a single test row
        print("Step 3: Inserting a single test record...")
        test_grant = {
            'foundation_ein': '00-TEST000',
            'tax_year': 2025,
            'recipient_name': 'TEST GRANT RECIPIENT',
            'grant_amount': 12345
        }
        cursor.execute("""
            INSERT INTO grants (foundation_ein, tax_year, recipient_name, grant_amount)
            VALUES (%(foundation_ein)s, %(tax_year)s, %(recipient_name)s, %(grant_amount)s);
        """, test_grant)
        conn.commit()
        print("  - Success: Inserted one record.")

        # 4. Count the records and print the result
        print("Step 4: Verifying the count...")
        cursor.execute("SELECT COUNT(*) FROM grants;")
        result = cursor.fetchone()
        print("\n----------------------------------------------------")
        print(f"RESULT FROM SCRIPT: The grants table now contains {result['count']} record.")
        print("----------------------------------------------------")

    except Exception as e:
        print(f"\nAN ERROR OCCURRED: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    test_connection()

