# precompute_scores.py (FINAL - Self-Contained Setup & Execution)

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from tqdm import tqdm
import numpy as np
import math
from collections import defaultdict

def main():
    conn = None
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL not found.")

    try:
        # --- Step 1: Ensure Table Exists and Permissions are Set ---
        print("--- Ensuring database is set up correctly... ---")
        conn = psycopg2.connect(db_url)
        with conn.cursor() as cursor:
            # Try to create the table, but don't fail if it's already there
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS foundation_scores (
                    foundation_ein TEXT PRIMARY KEY,
                    geo_score INT,
                    financial_score INT,
                    giving_velocity_score BIGINT,
                    national_funder_score INT,
                    smart_ask_amount NUMERIC
                );
            """)
            # Grant permissions just in case
            cursor.execute("GRANT ALL PRIVILEGES ON TABLE foundation_scores TO granterai_user;")
            conn.commit()
        conn.close()
        print("Database setup verified.")

        # --- Step 2: Run the Scoring Logic ---
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        print("--- Starting Pre-computation of Foundation Scores ---")

        with conn.cursor() as cursor:
            print("Fetching all foundations...")
            cursor.execute("SELECT ein, city, state, assets_fmv FROM foundations;")
            foundations = cursor.fetchall()
            
            print("Fetching all grant data...")
            cursor.execute("""
                SELECT g.foundation_ein, g.grant_amount, c.state AS recipient_state 
                FROM grants g
                JOIN charities c ON g.recipient_ein_matched = c.ein
                WHERE g.recipient_ein_matched IS NOT NULL;
            """)
            grants = cursor.fetchall()

        # (Calculation logic remains the same as it's proven to be fast)
        giving_velocity = defaultdict(float)
        national_funder = defaultdict(set)
        for grant in tqdm(grants, desc="Processing Grant Data"):
            ein = grant['foundation_ein']
            giving_velocity[ein] += (grant['grant_amount'] or 0)
            if grant['recipient_state']:
                national_funder[ein].add(grant['recipient_state'])

        smart_ask_amounts = {}
        grants_by_foundation = defaultdict(list)
        for grant in grants:
            if grant['grant_amount']:
                grants_by_foundation[grant['foundation_ein']].append(grant['grant_amount'])
        
        for ein, amounts in tqdm(grants_by_foundation.items(), desc="Calculating Ask Amounts"):
            if len(amounts) < 3:
                smart_ask_amounts[ein] = np.mean(amounts) if amounts else 0
                continue
            amounts.sort()
            trim_count = int(len(amounts) * 0.05)
            trimmed_amounts = amounts[trim_count:-trim_count] if trim_count > 0 else amounts
            smart_ask_amounts[ein] = np.mean(trimmed_amounts) if trimmed_amounts else 0

        scores_to_insert = []
        for f in foundations:
            ein = f['ein']
            assets = f.get('assets_fmv')
            financial_score = min(100, math.log10(assets) * 10) if assets and assets > 0 else 0
            num_states = len(national_funder.get(ein, set()))
            national_score = 100 if num_states > 10 else (50 if num_states >= 5 else 0)
            
            scores_to_insert.append({
                'foundation_ein': ein,
                'geo_score': 0, 'financial_score': int(financial_score),
                'giving_velocity_score': int(giving_velocity.get(ein, 0)),
                'national_funder_score': national_score,
                'smart_ask_amount': smart_ask_amounts.get(ein, 0)
            })

        print(f"\nFound {len(scores_to_insert)} foundations to score. Updating database...")
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE foundation_scores;")
            execute_batch(cursor,
                """
                INSERT INTO foundation_scores (foundation_ein, geo_score, financial_score, giving_velocity_score, national_funder_score, smart_ask_amount)
                VALUES (%(foundation_ein)s, %(geo_score)s, %(financial_score)s, %(giving_velocity_score)s, %(national_funder_score)s, %(smart_ask_amount)s)
                ON CONFLICT (foundation_ein) DO UPDATE SET
                    financial_score = EXCLUDED.financial_score,
                    giving_velocity_score = EXCLUDED.giving_velocity_score,
                    national_funder_score = EXCLUDED.national_funder_score,
                    smart_ask_amount = EXCLUDED.smart_ask_amount;
                """,
                scores_to_insert
            )
            conn.commit()
            print("Successfully pre-computed and stored foundation scores.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
