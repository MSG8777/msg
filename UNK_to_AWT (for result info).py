import pandas as pd
import os

# --- CONFIGURATION ---
# Update this path to where your file is currently located
OUTPUT_DIR = r"C:\HKJC_gemini_3_horse_racing_ML" 
FILE_NAME = "hkjc_race_results_v9_COURSES.csv"
FULL_PATH = os.path.join(OUTPUT_DIR, FILE_NAME)

def fix_unk_courses():
    print(f"Reading file: {FULL_PATH}...")
    
    if not os.path.exists(FULL_PATH):
        print("Error: File not found. Please check the path.")
        return

    # Read the CSV
    df = pd.read_csv(FULL_PATH)
    
    # Count UNK before
    unk_count_before = len(df[df['Course'] == 'UNK'])
    print(f"Total 'UNK' courses before fix: {unk_count_before}")
    
    # --- APPLY LOGIC: IF Course is UNK AND Venue is ST -> CHANGE TO AWT ---
    mask = (df['Course'] == 'UNK') & (df['Venue'] == 'ST')
    df.loc[mask, 'Course'] = 'AWT'
    
    # Count UNK after
    unk_count_after = len(df[df['Course'] == 'UNK'])
    changed_count = unk_count_before - unk_count_after
    
    print(f"Rows updated (UNK -> AWT): {changed_count}")
    print(f"Remaining 'UNK' courses: {unk_count_after}")
    
    # Save back to the same file
    print("Saving file...")
    df.to_csv(FULL_PATH, index=False)
    print("Done! File updated successfully.")

if __name__ == "__main__":
    fix_unk_courses()

