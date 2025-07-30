# generate_embeddings.py (FINAL VERSION)

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from sentence_transformers import SentenceTransformer
from pgvector.psycopg2 import register_vector
from tqdm import tqdm
import numpy as np

# --- CONFIGURATION ---
MODEL_NAME = 'all-MiniLM-L6-v2'

def main():
    print("--- Starting Final Embedding Generation ---")
    conn = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL not found.")
            
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        register_vector(conn) # Enable the pgvector type for this connection

        print(f"Loading AI model: '{MODEL_NAME}'... (This may take a moment)")
        model = SentenceTransformer(MODEL_NAME)
        print("Model loaded successfully.")

        with conn.cursor() as cursor:
            print("Finding all grants that need embeddings...")
            cursor.execute("SELECT id, grant_purpose FROM grants WHERE grant_purpose IS NOT NULL AND embedding IS NULL")
            all_rows = cursor.fetchall()

        if not all_rows:
            print("All grant embeddings are already up to date.")
            return

        print(f"Found {len(all_rows)} grants to embed. Processing...")
        
        grant_texts = [row['grant_purpose'] for row in all_rows]
        
        # Generate all embeddings at once for maximum efficiency
        embeddings = model.encode(grant_texts, show_progress_bar=True)
        
        updates_to_make = []
        for i, row in enumerate(all_rows):
            updates_to_make.append((np.array(embeddings[i]), row['id']))

        print(f"Embeddings generated. Updating {len(updates_to_make)} records in the database...")
        
        with conn.cursor() as cursor:
            execute_batch(cursor, "UPDATE grants SET embedding = %s WHERE id = %s", updates_to_make)
            conn.commit()

        print(f"\n--- Success! {cursor.rowcount} grant embeddings are now stored in the database. ---")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()
