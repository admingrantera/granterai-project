# process_final_data.py

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import google.generativeai as genai
from sentence_transformers import SentenceTransformer
from pgvector.psycopg2 import register_vector
from tqdm import tqdm
import numpy as np

# --- CONFIGURATION ---
MODEL_NAME = 'all-MiniLM-L6-v2'

def get_db_connection():
    db_url = os.environ.get("DATABASE_URL") + "?sslmode=require"
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor)

def main():
    conn = None
    try:
        conn = get_db_connection()
        register_vector(conn)
        
        # --- Step 1: Generate Missing Purposes with AI ---
        print("--- Step 1: Generating missing grant purposes with AI... ---")
        with conn.cursor() as cursor:
            # Find grants that have been matched but are missing a purpose
            cursor.execute("""
                SELECT g.id, f.name AS foundation_name, f.mission_statement, c.name AS recipient_name
                FROM grants g
                JOIN foundations f ON g.foundation_ein = f.ein
                JOIN charities c ON g.recipient_ein_matched = c.ein
                WHERE g.grant_purpose IS NULL AND g.recipient_ein_matched IS NOT NULL;
            """)
            tasks = cursor.fetchall()

        if not tasks:
            print("No grants need a purpose generated.")
        else:
            print(f"Found {len(tasks)} matched grants with missing purposes. Generating with AI...")
            genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
            model = genai.GenerativeModel('gemini-1.5-flash')
            
            updates_to_make = []
            for task in tqdm(tasks, desc="Generating Purposes"):
                prompt = f"""
                A foundation named "{task['foundation_name']}" (mission: "{task['mission_statement']}") 
                gave a grant to "{task['recipient_name']}". 
                What was the grant's likely purpose? Be concise. Default to 'General charitable purposes' if unsure.
                """
                try:
                    response = model.generate_content(prompt)
                    updates_to_make.append((response.text.strip(), task['id']))
                except Exception:
                    continue
            
            if updates_to_make:
                with conn.cursor() as cursor:
                    execute_batch(cursor, "UPDATE grants SET grant_purpose = %s WHERE id = %s", updates_to_make)
                    conn.commit()
                print(f"Successfully generated and saved {len(updates_to_make)} new purposes.")

        # --- Step 2: Generate Embeddings for ALL grants with a purpose ---
        print("\n--- Step 2: Generating final embeddings for all usable grants... ---")
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, grant_purpose FROM grants WHERE grant_purpose IS NOT NULL AND embedding IS NULL")
            grants_to_embed = cursor.fetchall()
        
        if not grants_to_embed:
            print("All grant embeddings are up to date.")
        else:
            print(f"Found {len(grants_to_embed)} grants to embed...")
            model = SentenceTransformer(MODEL_NAME)
            purposes = [g['grant_purpose'] for g in grants_to_embed]
            embeddings = model.encode(purposes, show_progress_bar=True)
            
            embedding_updates = [(np.array(emb), g['id']) for emb, g in zip(embeddings, grants_to_embed)]
            
            with conn.cursor() as cursor:
                execute_batch(cursor, "UPDATE grants SET embedding = %s WHERE id = %s", embedding_updates)
                conn.commit()
            print(f"Successfully generated and stored {len(embedding_updates)} embeddings.")

        print("\n--- Final data processing complete. ---")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
