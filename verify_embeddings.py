# verify_embeddings.py

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
        
        print("--- Verifying AI Embedding Generation ---")

        with conn.cursor() as cursor:
            # Count how many grants have a purpose (these should have an embedding)
            cursor.execute("SELECT COUNT(*) FROM grants WHERE grant_purpose IS NOT NULL;")
            purpose_count = cursor.fetchone()['count']

            # Count how many grants actually have an embedding
            cursor.execute("SELECT COUNT(*) FROM grants WHERE embedding IS NOT NULL;")
            embedding_count = cursor.fetchone()['count']

            print("\n--- Verification Results ---")
            print(f"Grants with a purpose: {purpose_count}")
            print(f"Grants with an embedding: {embedding_count}")

            if purpose_count == 0:
                 print("\n[WARNING] No grants with a purpose were found. The parser may not have captured grant purposes correctly.")
            elif purpose_count == embedding_count:
                print("\n[SUCCESS] The numbers match perfectly. All eligible grants have been successfully embedded.")
            else:
                print(f"\n[FAILURE] The numbers do not match. {purpose_count - embedding_count} grants are missing their embedding.")
    
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
