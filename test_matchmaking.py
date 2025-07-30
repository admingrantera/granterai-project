# test_matchmaking.py (FINAL - Includes Smart Ask Amount)

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from sentence_transformers import SentenceTransformer
from pgvector.psycopg2 import register_vector
import numpy as np

# --- CONFIGURATION ---
TEST_MISSION_STATEMENT = "Our mission is to provide after-school arts and music education to underprivileged youth in urban communities."

def find_matches():
    """Finds and prints the top 10 unique foundation matches, including their Smart Ask Amount."""
    conn = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        register_vector(conn)
        cursor = conn.cursor()

        print("--- Testing the Full Matchmaking Pipeline (with Smart Ask Amount) ---")
        print(f"\nSearching for matches for mission: '{TEST_MISSION_STATEMENT}'")

        print("Loading AI model...")
        retriever = SentenceTransformer('all-MiniLM-L6-v2')
        profile_embedding = retriever.encode(TEST_MISSION_STATEMENT)

        # --- THIS IS THE UPGRADED QUERY ---
        # It now joins with the foundation_scores table to retrieve the smart_ask_amount.
        query = """
            WITH top_grants AS (
                SELECT
                    foundation_ein,
                    1 - (embedding <=> %s) AS similarity
                FROM grants
                WHERE embedding IS NOT NULL
                ORDER BY embedding <=> %s
                LIMIT 250
            ),
            ranked_foundations AS (
                SELECT
                    foundation_ein,
                    AVG(similarity) AS avg_similarity
                FROM top_grants
                GROUP BY foundation_ein
            )
            SELECT
                f.name,
                f.city,
                f.state,
                rf.avg_similarity,
                fs.smart_ask_amount
            FROM ranked_foundations rf
            JOIN foundations f ON rf.foundation_ein = f.ein
            JOIN foundation_scores fs ON rf.foundation_ein = fs.foundation_ein
            ORDER BY rf.avg_similarity DESC
            LIMIT 10;
        """
        cursor.execute(query, (np.array(profile_embedding), np.array(profile_embedding)))
        matches = cursor.fetchall()
        
        if not matches:
            print("\n--- No matches found. ---")
            return

        print("\n--- Top 10 Unique Matches Found ---")
        print(f"{'Foundation Name':<50} {'Location':<20} {'Avg. Match Score':<20} {'Smart Ask Amount'}")
        print("-" * 110)
        for match in matches:
            location = f"{match.get('city', 'N/A')}, {match.get('state', 'N/A')}"
            score = f"{round(match['avg_similarity'] * 100)}%"
            ask_amount = f"${int(match['smart_ask_amount']):,}" if match['smart_ask_amount'] else "N/A"
            print(f"{match['name'][:48]:<50} {location:<20} {score:<20} {ask_amount}")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    find_matches()
