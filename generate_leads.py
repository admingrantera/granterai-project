import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load database credentials from .env file
load_dotenv()

def get_db_connection():
    """Establishes and returns a database connection using the DATABASE_URL."""
    # THIS FUNCTION HAS BEEN CORRECTED
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL not found in .env file.")
    if 'sslmode' not in db_url:
        db_url += "?sslmode=require"
    return psycopg2.connect(db_url)

def main():
    """Finds new charities and populates the crm_leads table."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # 1. Get all existing lead EINs for a quick lookup
            cur.execute("SELECT ein FROM crm_leads WHERE ein IS NOT NULL;")
            existing_eins = {row[0] for row in cur.fetchall()}
            print(f"Found {len(existing_eins)} existing leads in the CRM.")

            # 2. Get all potential leads from your main charities table
            # THIS QUERY HAS BEEN CORRECTED
            cur.execute("""
                SELECT DISTINCT ON (ein)
                    ein, name, city, state
                FROM charities
                WHERE ein IS NOT NULL;
            """)
            potential_leads_cursor = cur.fetchall()
            potential_leads = [(lead[0], lead[1], lead[2], lead[3]) for lead in potential_leads_cursor]


            # 3. Filter out the charities that already exist
            new_leads_to_insert = []
            for lead in potential_leads:
                ein = lead[0]
                if ein not in existing_eins:
                    new_leads_to_insert.append(lead)

            if not new_leads_to_insert:
                print("No new leads found to insert.")
                return

            print(f"Found {len(new_leads_to_insert)} new leads to insert.")

            # 4. Use execute_values for a single, efficient bulk INSERT
            execute_values(
                cur,
                "INSERT INTO crm_leads (ein, name, city, state) VALUES %s",
                new_leads_to_insert,
                page_size=1000
            )

            # 5. Commit the transaction
            conn.commit()
            print("Successfully inserted new leads into crm_leads.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
