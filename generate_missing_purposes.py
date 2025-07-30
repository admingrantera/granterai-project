# generate_missing_purposes.py (Corrected Update Logic)

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import google.generativeai as genai
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

# --- CONFIGURATION ---
NUM_PROCESSES = max(1, cpu_count() - 1)

# --- AI & DB CONFIG (GLOBAL FOR WORKERS) ---
genai_model = None
db_connection_string = None

def init_worker(api_key, db_dsn):
    """Initializes API key and DB connection string for each worker process."""
    global genai_model, db_connection_string
    genai.configure(api_key=api_key)
    try:
        genai_model = genai.GenerativeModel('gemini-1.5-flash')
    except Exception as e:
        print(f"WORKER ERROR: Failed to initialize GenerativeModel: {e}")
        genai_model = None
    db_connection_string = db_dsn

def generate_purpose(task_data):
    """
    Takes a foundation's mission and a recipient's name, and returns an AI-generated purpose.
    """
    if not genai_model:
        return None

    foundation_mission = task_data.get('mission_statement')
    recipient_name = task_data.get('recipient_name')
    foundation_name = task_data.get('foundation_name')

    if not foundation_mission or not recipient_name or not foundation_name:
        return None

    prompt = f"""
    Analyze the following information:
    1. A foundation named "{foundation_name}" has a mission: "{foundation_mission}"
    2. This foundation gave a grant to an organization named: "{recipient_name}"

    Based only on this context, write a single, concise sentence describing the grant's likely purpose.
    Phrase the purpose as a general activity. For example, instead of 'To help the museum', write 'To support arts and cultural programs'.
    If the recipient's name gives no specific clue, a good default is 'For general charitable purposes'.

    Output only the single sentence of the generated purpose.
    """

    try:
        response = genai_model.generate_content(prompt)
        # Clean up the AI's response by removing quotes or extra whitespace
        return response.text.strip().strip('"')
    except Exception:
        return None

def main():
    print("--- Starting AI Purpose Generation ---")
    conn = None
    try:
        db_dsn = os.environ.get("DATABASE_URL")
        if not db_dsn:
            raise ValueError("DATABASE_URL not found in .env file.")
        conn = psycopg2.connect(db_dsn, cursor_factory=RealDictCursor)

        print("Finding unique pairs with missing grant purposes...")
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT g.foundation_ein, f.name AS foundation_name, f.mission_statement, c.name AS recipient_name, g.recipient_ein_matched
                FROM grants g
                JOIN foundations f ON g.foundation_ein = f.ein
                JOIN charities c ON g.recipient_ein_matched = c.ein
                WHERE g.grant_purpose IS NULL AND g.recipient_ein_matched IS NOT NULL;
            """)
            tasks = cursor.fetchall()

        if not tasks:
            print("No grants with missing purposes found. All data is complete.")
            return

        print(f"Found {len(tasks)} unique foundation/recipient pairs to process with AI. Starting parallel processing...")

        gemini_api_key = os.environ.get("GEMINI_API_KEY")
        if not gemini_api_key:
            print("ERROR: GEMINI_API_KEY not found in .env file.")
            return

        with Pool(processes=NUM_PROCESSES, initializer=init_worker, initargs=(gemini_api_key, db_dsn)) as pool:
            results = list(tqdm(pool.imap_unordered(generate_purpose, tasks), total=len(tasks), desc="Generating Purposes"))

        purpose_map = {}
        successful_generations = 0
        for task, generated_purpose in zip(tasks, results):
            if generated_purpose:
                successful_generations += 1
                # The key is now the unique combination of the foundation and the matched recipient's EIN
                key = (task['foundation_ein'], task['recipient_ein_matched'])
                if key not in purpose_map:
                    purpose_map[key] = generated_purpose
        
        if not purpose_map:
            print("AI failed to generate any new purposes.")
            return

        print(f"\nAI successfully generated {successful_generations} new purposes for {len(purpose_map)} unique pairs. Updating database...")
        
        update_data = []
        for (foundation_ein, recipient_ein), purpose in purpose_map.items():
            update_data.append((purpose, foundation_ein, recipient_ein))

        if update_data:
            with conn.cursor() as cursor:
                # --- THIS IS THE CORRECTED UPDATE LOGIC ---
                # It now updates all grants for a given foundation and matched recipient EIN
                execute_batch(cursor,
                  "UPDATE grants SET grant_purpose = %s WHERE foundation_ein = %s AND recipient_ein_matched = %s AND grant_purpose IS NULL",
                  update_data
                )
                conn.commit()
                print(f"Successfully updated {cursor.rowcount} grant records.")

        print("--- AI Enrichment Complete. ---")

    except Exception as e:
        print(f"\nAn error occurred in the main process: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
