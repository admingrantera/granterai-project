# diagnose_xml_tags.py

import os
import xml.etree.ElementTree as ET
from tqdm import tqdm
from collections import Counter
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

def get_all_tags(filepath):
    """
    Parses a single XML file and returns a list of all unique tag names within it.
    """
    try:
        # Use iterparse for memory efficiency, though it's not strictly necessary for this sample size.
        tags = set()
        for event, elem in ET.iterparse(filepath):
            # Clean the namespace from the tag name
            tag_name = re.sub(r'\{.*\}', '', elem.tag)
            tags.add(tag_name)
            elem.clear() # Free memory
        return tags
    except Exception:
        return None

def main():
    print(f"--- Diagnosing XML tags from a sample of {SAMPLE_SIZE} files ---")

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
    tag_counter = Counter()

    for filepath in tqdm(files_to_analyze, desc="Analyzing XML Tags"):
        unique_tags = get_all_tags(filepath)
        if unique_tags:
            parsable_count += 1
            tag_counter.update(unique_tags)

    print("\n--- Analysis Complete ---")
    print(f"Total files analyzed: {total_files}")
    print(f"Successfully parsed: {parsable_count}/{total_files}")
    
    print("\n--- Found the following unique XML tags (and their frequency): ---")
    # Print the most common tags first
    for tag, count in tag_counter.most_common():
        print(f"{tag:<50} found in {count} files")

    # Specifically check for our target tags
    print("\n--- Grant and Officer Tag Analysis ---")
    grant_tags = ['GrantOrContributionPdDurYrGrp', 'RecipientBusinessName', 'RecipientUSAddress', 'Amt']
    officer_tags = ['Form990PartVIISectionAGrp', 'OfficerDirTrstKeyEmplGrp', 'BusinessOfficerGrp', 'PersonNm', 'TitleTxt']
    
    print("\nGrant-related tags found:")
    for tag in grant_tags:
        if tag in tag_counter:
            print(f"  - '{tag}' found in {tag_counter[tag]} files.")
        else:
            print(f"  - '{tag}' NOT FOUND in sample.")

    print("\nOfficer-related tags found:")
    for tag in officer_tags:
        if tag in officer_tags:
            print(f"  - '{tag}' found in {tag_counter[tag]} files.")
        else:
            print(f"  - '{tag}' NOT FOUND in sample.")

if __name__ == "__main__":
    main()

