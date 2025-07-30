# analyze_xml_content.py (FINAL, CORRECTED VERSION)

import os
import xml.etree.ElementTree as ET
from tqdm import tqdm
import re

# --- CONFIGURATION ---
FILE_LIST_PATH = "file_list.txt"
SAMPLE_SIZE = 1000 # We'll analyze the first 1,000 files for speed.

def convert_windows_path_to_wsl(path):
    """Converts a Windows path to a format Ubuntu can use."""
    path = path.strip()
    if path.startswith('C:'):
        return '/mnt/c' + path[2:].replace('\\', '/')
    return path

def analyze_file(filepath):
    """
    Analyzes a single XML file to see if it contains grant data
    using the correct namespace-aware method.
    Returns a tuple: (was_parsable, has_grants, xml_content_if_has_grants)
    """
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        
        # --- THIS IS THE CORRECTED LOGIC ---
        # Define the namespace dictionary. The key ('irs') can be anything,
        # but the value must match the URI from the XML file.
        ns = {'irs': 'http://www.irs.gov/efile'}

        # Use the namespace dictionary in the findall method.
        grants = root.findall('.//irs:GrantOrContributionPdDurYrGrp', ns)
        
        if grants:
            # If grants are found, return the full XML text for inspection
            return (True, True, ET.tostring(root, encoding='unicode'))
        else:
            # The file was parsed, but no grant tags were found
            return (True, False, None)
            
    except (ET.ParseError, FileNotFoundError):
        # The file was corrupted, not valid XML, or not found
        return (False, False, None)
    except Exception:
        # Any other error
        return (False, False, None)

def main():
    print(f"--- Analyzing a sample of {SAMPLE_SIZE} XML files (Corrected Namespace Version) ---")

    try:
        with open(FILE_LIST_PATH, 'r') as f:
            all_files = [convert_windows_path_to_wsl(line) for line in f]
    except FileNotFoundError:
        print(f"ERROR: The file list '{FILE_LIST_PATH}' was not found.")
        return

    if not all_files:
        print("The file list is empty.")
        return
    
    files_to_analyze = all_files[:SAMPLE_SIZE]

    total_files = len(files_to_analyze)
    parsable_count = 0
    grant_count = 0
    grant_examples = []

    for filepath in tqdm(files_to_analyze, desc="Analyzing Files"):
        parsable, has_grants, content = analyze_file(filepath)
        if parsable:
            parsable_count += 1
        if has_grants:
            grant_count += 1
            if len(grant_examples) < 3: # Save the first 3 examples
                grant_examples.append(content)

    print("\n--- Analysis Complete ---")
    print(f"Total files analyzed: {total_files}")
    print(f"Successfully parsed: {parsable_count}/{total_files}")
    print(f"Files containing grant data: {grant_count}/{total_files}")

    if grant_examples:
        print("\n--- Example XML from a File Containing Grants ---")
        for i, example in enumerate(grant_examples):
            print(f"\n--- EXAMPLE {i+1} ---")
            # Print a snippet of the XML for review
            print(example[:2000] + "\n...")
    else:
        print("\nNo grant data was found in the sample.")

if __name__ == "__main__":
    main()

