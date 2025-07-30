# test_embeddings.py

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from sentence_transformers import SentenceTransformer
from pgvector.psycopg2 import register_vector
from tqdm import tqdm

MODEL_NAME = 'all-MiniLM-L6-v2'
BATCH_SIZE = 250 # Smaller batch size for the test

def generate_and_store_embeddings():
    print("--- Starting Embedding Test Process ---")
    conn = None
    try:
        database_url = os.environ.get("DATABASE_URL") + "?sslmode=require"
        conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        register_vector(conn)
        cursor = conn.cursor()

        print(f"Loading AI model: '{MODEL_NAME}'...")
        model = SentenceTransformer(MODEL_NAME)
        print("Model loaded.")

        # THIS IS THE ONLY CHANGE: WE ADD "LIMIT 1000"
        print("Fetching a sample of 1,000 grants for the test...")
        cursor.execute("SELECT id, grant_purpose FROM grants WHERE grant_purpose IS NOT NULL AND embedding IS NULL LIMIT 1000")
        all_rows = cursor.fetchall()

        if not all_rows:
            print("No grants found to test. Ensure previous steps are complete.")
            return

        print(f"Found {len(all_rows)} grants to embed. Processing in batches...")
        
        for i in tqdm(range(0, len(all_rows), BATCH_SIZE), desc="Generating Test Embeddings"):
            batch = all_rows[i:i + BATCH_SIZE]
            grant_ids = [row['id'] for row in batch]
            grant_texts = [row['grant_purpose'] for row in batch]

            embeddings = model.encode(grant_texts, show_progress_bar=False)

            for grant_id, embedding in zip(grant_ids, embeddings):
                cursor.execute(
                    "UPDATE grants SET embedding = %s WHERE id = %s",
                    (embedding, grant_id)
                )
            conn.commit()

        print("\n--- Test embedding process complete. ---")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    generate_and_store_embeddings()
