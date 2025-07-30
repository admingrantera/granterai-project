# build_master_charities.py (Multi-File Version)

import pandas as pd
from tqdm import tqdm

# --- CONFIGURATION ---
IRS_RAW_DATA_FILES = ["eo1.csv", "eo2.csv", "eo3.csv", "eo4.csv"]
OUTPUT_FILE = "master_charities.csv"

def main():
    print(f"--- Building new master charity list from all 4 IRS regional files ---")

    try:
        columns_to_keep = {
            'EIN': 'ein',
            'NAME': 'name',
            'CITY': 'city',
            'STATE': 'state'
        }

        total_rows = 0

        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f_out:
            f_out.write(','.join(columns_to_keep.values()) + '\n')

        for filename in IRS_RAW_DATA_FILES:
            print(f"\nProcessing file: {filename}...")
            try:
                with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f_out:
                    chunk_size = 50000
                    with pd.read_csv(filename, chunksize=chunk_size, low_memory=False, usecols=columns_to_keep.keys()) as reader:
                        for chunk in tqdm(reader, desc=f"Reading {filename}"):
                            chunk.rename(columns=columns_to_keep, inplace=True)
                            chunk.dropna(subset=['ein', 'name'], inplace=True)
                            chunk['ein'] = pd.to_numeric(chunk['ein'], errors='coerce').dropna().astype(int)
                            chunk['ein'] = chunk['ein'].astype(str).str.zfill(9)
                            chunk.to_csv(f_out, header=False, index=False)
                            total_rows += len(chunk)
            except FileNotFoundError:
                print(f"WARNING: File '{filename}' not found. Skipping.")
                continue

        print(f"\n--- Success! Created '{OUTPUT_FILE}' with a total of {total_rows:,} records. ---")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
