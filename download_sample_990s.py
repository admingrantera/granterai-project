# download_sample_990s.py (Definitive Direct Link Version)

import os
import pandas as pd
import requests
from tqdm import tqdm

# --- CONFIGURATION ---
SAMPLE_SIZE = 200
OUTPUT_DIRECTORY = "sample_990_xmls"

# This is a stable, direct link to the community-maintained master index file.
INDEX_URL = "https://raw.githubusercontent.com/Nonprofit-Open-Data-Collective/irs-990-efile-index/master/index.csv"
S3_BUCKET_URL = "https://s3.amazonaws.com/irs-form-990"

def main():
    print(f"--- Downloading a sample of {SAMPLE_SIZE} Form 990 XML files ---")
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

    try:
        print(f"Loading master index file from reliable source: {INDEX_URL}")
        
        # Load the index file directly into a pandas DataFrame
        index_df = pd.read_csv(INDEX_URL, usecols=['RETURN_TYPE', 'OBJECT_ID', 'SUBMISSION_DATE'])
        print("Master index loaded successfully.")

        # Filter for only public charities (990) and get the most recent filings
        form_990_df = index_df[index_df['RETURN_TYPE'] == '990'].sort_values(by='SUBMISSION_DATE', ascending=False)
        sample_df = form_990_df.head(SAMPLE_SIZE)

        print(f"\nFound {len(sample_df)} recent Form 990 filings to download.")

        # Download the sample files
        for index, row in tqdm(sample_df.iterrows(), total=sample_df.shape[0], desc="Downloading Samples"):
            try:
                object_id = str(int(row['OBJECT_ID']))
            except ValueError:
                continue 

            xml_url = f"{S3_BUCKET_URL}/{object_id}_public.xml"
            filepath = os.path.join(OUTPUT_DIRECTORY, f"{object_id}_public.xml")

            try:
                response = requests.get(xml_url, timeout=30)
                if response.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
            except requests.exceptions.RequestException:
                continue
                
        print(f"\n--- Success! Downloaded a sample of files to the '{OUTPUT_DIRECTORY}' folder. ---")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
