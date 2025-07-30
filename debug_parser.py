# debug_parser.py

from dotenv import load_dotenv
load_dotenv()

import os
import xml.etree.ElementTree as ET
import psycopg2
import traceback
import sys

def convert_windows_path_to_wsl(path):
    """Converts a Windows path to a format Ubuntu can use."""
    path = path.strip()
    if path.startswith('C:'):
        return '/mnt/c' + path[2:].replace('\\', '/')
    return path

def debug_single_file(filepath):
    """
    Parses a single 990-PF file with detailed print statements to find the point of failure.
    """
    print(f"\n--- Starting Debug for file: {os.path.basename(filepath)} ---")
    try:
        # 1. Attempt to parse the XML file
        print("Step 1: Parsing XML file...")
        tree = ET.parse(filepath)
        root = tree.getroot()
        ns = {'irs': 'http://www.irs.gov/efile'}
        print("  - Success: XML file parsed.")

        def find_text(path, parent=root):
            element = parent.find(path, ns)
            return element.text.strip() if element is not None and element.text else None

        # 2. Extract EIN
        print("Step 2: Finding EIN...")
        ein = find_text('.//irs:Filer/irs:EIN')
        if not ein:
            print("  - FAILURE: Could not find EIN. Stopping.")
            return
        print(f"  - Success: Found EIN: {ein}")

        # 3. Extract Officers
        print("Step 3: Finding Officers...")
        officers_data = []
        officer_elements = root.findall('.//irs:Form990PartVIISectionAGrp', ns)
        for person in officer_elements:
            officers_data.append({'org_ein': ein, 'name': find_text('.//irs:PersonNm', parent=person), 'title': find_text('.//irs:TitleTxt', parent=person), 'org_type': 'foundation'})
        print(f"  - Success: Found {len(officers_data)} officer records.")

        # 4. Extract Grants
        print("Step 4: Finding Grants...")
        grants_data = []
        grant_elements = root.findall('.//irs:GrantOrContributionPdDurYrGrp', ns)
        for grant in grant_elements:
            grants_data.append({
                'foundation_ein': ein,
                'recipient_name': find_text('.//irs:RecipientBusinessName/irs:BusinessNameLine1Txt', parent=grant),
                'grant_amount': find_text('.//irs:Amt', parent=grant) # Keep as string for now
            })
        print(f"  - Success: Found {len(grants_data)} grant records.")

        if not officers_data and not grants_data:
            print("\n--- RESULT: File parsed successfully, but no officer or grant data was found to insert. ---")
            return

        # 5. Attempt to connect to the database
        print("Step 5: Connecting to database...")
        db_dsn = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_dsn)
        cursor = conn.cursor()
        print("  - Success: Database connection established.")

        # 6. Attempt to insert data
        print("Step 6: Inserting data into database...")
        # (This part is commented out to prevent writing partial data during debug)
        # with conn.cursor() as cursor:
        #     # ... insertion logic ...
        # conn.commit()
        print("  - (Skipping actual insert for debug purposes)")
        
        print("\n--- RESULT: Script completed all steps successfully for this file. ---")

    except Exception as e:
        print(f"\n--- AN ERROR OCCURRED ---")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {e}")
        print("Full Traceback:")
        traceback.print_exc()
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == '__main__':
    # We need the path to a single file to test.
    # We will take it from the command line.
    if len(sys.argv) < 2:
        print("Usage: python3 debug_parser.py <full_path_to_xml_file>")
        # Let's try to find the file list and use the first file as a default
        FILE_LIST_PATH = "file_list.txt"
        try:
            with open(FILE_LIST_PATH, 'r') as f:
                first_file = f.readline().strip()
                if first_file:
                    print(f"No file provided. Using first file from list: {first_file}")
                    wsl_path = convert_windows_path_to_wsl(first_file)
                    debug_single_file(wsl_path)
        except FileNotFoundError:
            print(f"Could not find '{FILE_LIST_PATH}' to get a sample file.")
    else:
        file_to_debug = sys.argv[1]
        debug_single_file(file_to_debug)

