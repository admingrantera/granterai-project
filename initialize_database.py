# initialize_database.py

from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2

def main():
    conn = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL not found.")
        conn = psycopg2.connect(db_url)

        print("--- Initializing Database Tables ---")

        with conn.cursor() as cursor:
            print("Creating tables if they do not exist...")

            # This single block of SQL contains all table creation logic.
            # 'IF NOT EXISTS' prevents errors if the tables are already there.
            create_script = """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    plan TEXT DEFAULT 'trial',
                    trial_end_date DATE,
                    stripe_customer_id TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS foundations (
                    ein TEXT PRIMARY KEY,
                    name TEXT,
                    address_line_1 TEXT,
                    city TEXT,
                    state TEXT,
                    zip_code TEXT,
                    assets_fmv BIGINT,
                    mission_statement TEXT
                );

                CREATE TABLE IF NOT EXISTS charities (
                    ein TEXT PRIMARY KEY,
                    name TEXT,
                    city TEXT,
                    state TEXT,
                    address_line_1 TEXT,
                    zip_code TEXT,
                    mission_statement TEXT,
                    normalized_name TEXT
                );

                CREATE TABLE IF NOT EXISTS charity_profiles (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER UNIQUE NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    charity_name TEXT,
                    charity_ein TEXT,
                    mission_statement TEXT
                );

                CREATE TABLE IF NOT EXISTS grants (
                    id SERIAL PRIMARY KEY,
                    foundation_ein TEXT REFERENCES foundations(ein) ON DELETE CASCADE,
                    recipient_name TEXT,
                    grant_amount NUMERIC,
                    grant_purpose TEXT,
                    tax_year INTEGER,
                    recipient_ein TEXT,
                    recipient_ein_matched TEXT,
                    normalized_name TEXT,
                    embedding public.vector(384)
                );
            """
            cursor.execute(create_script)
            conn.commit()

        print("--- Database Initialization Complete ---")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
