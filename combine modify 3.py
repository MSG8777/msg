import pandas as pd
import numpy as np
import os

# ==========================================
# CONFIGURATION
# ==========================================
WORKING_DIR = r'C:\HKJC_gemini_3_horse_racing_ML\modify_csv'
INPUT_FILE = 'hkjc_race_results_MERGED_FINAL.csv'
OUTPUT_FILE = 'hkjc_race_results_MERGED_FINAL_detailed.csv'

# The most statistically significant gears in HK racing
IMPORTANT_GEARS = ['B', 'TT', 'V', 'CP', 'H']

def main():
    input_path = os.path.join(WORKING_DIR, INPUT_FILE)
    output_path = os.path.join(WORKING_DIR, OUTPUT_FILE)

    print(f"Loading data from: {input_path}")
    try:
        df = pd.read_csv(input_path)
    except FileNotFoundError:
        print(f"Error: File not found at {input_path}")
        return

    # --- Preprocessing ---
    df['declared_wt'] = pd.to_numeric(df['declared_wt'], errors='coerce')
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by=['horse_code', 'date'])

    # --- Lag Features ---
    print("Calculating previous race stats...")
    grouped = df.groupby('horse_code')
    
    df['prev_actual_wt'] = grouped['actual_wt'].shift(1)
    df['prev_declared_wt'] = grouped['declared_wt'].shift(1)
    df['prev_rating'] = grouped['rating'].shift(1)
    df['prev_gear'] = grouped['gear'].shift(1)
    df['prev_race_class'] = grouped['race_class'].shift(1)

    # --- Numeric Changes ---
    df['actual_wt_change'] = df['actual_wt'] - df['prev_actual_wt']
    df['declared_wt_change'] = df['declared_wt'] - df['prev_declared_wt']
    df['rating_change'] = df['rating'] - df['prev_rating']

    # --- Gear Analysis (Generic & Specific) ---
    print("Analyzing specific gear changes...")
    
    curr_gears = df['gear'].fillna('').astype(str).tolist()
    prev_gears = df['prev_gear'].fillna('').astype(str).tolist()

    # Initialize lists for generic changes
    gear_added_list = []
    gear_dropped_list = []
    
    # Initialize dictionaries for specific gear changes
    # This creates a list of 0s/1s for each important gear
    specific_changes = {f'gear_{g}_{action}': [] for g in IMPORTANT_GEARS for action in ['added', 'dropped']}

    for curr, prev in zip(curr_gears, prev_gears):
        # 1. Parse current and previous gear sets
        c_set = set([x.strip() for x in curr.split('/') if x.strip()])
        p_set = set([x.strip() for x in prev.split('/') if x.strip()])
        
        # 2. Generic Check
        added_items = c_set - p_set
        dropped_items = p_set - c_set
        
        gear_added_list.append(1 if len(added_items) > 0 else 0)
        gear_dropped_list.append(1 if len(dropped_items) > 0 else 0)

        # 3. Specific Gear Checks (The "Big 5")
        for g in IMPORTANT_GEARS:
            # Check if specific gear 'g' is in the added set
            specific_changes[f'gear_{g}_added'].append(1 if g in added_items else 0)
            # Check if specific gear 'g' is in the dropped set
            specific_changes[f'gear_{g}_dropped'].append(1 if g in dropped_items else 0)

    # Assign Generic Columns
    df['gear_added'] = gear_added_list
    df['gear_dropped'] = gear_dropped_list

    # Assign Specific Columns
    for col_name, values in specific_changes.items():
        df[col_name] = values

    # --- Class Analysis ---
    print("Analyzing class changes...")
    class_rank_map = {
        'Group 1': 1, 'Group 2': 2, 'Group 3': 3,
        'Open': 4, 'Class 1': 4,
        'Class 2': 5, 'Class 3': 6, 'Class 4': 7,
        'Class 5': 8, 'Class 6': 9, 'Class 7': 10, 'Griffin': 11
    }

    df['curr_rank'] = df['race_class'].map(class_rank_map)
    df['prev_rank'] = df['prev_race_class'].map(class_rank_map)

    df['class_up'] = np.where((df['prev_rank'] > df['curr_rank']), 1, 0)
    df['class_down'] = np.where((df['prev_rank'] < df['curr_rank']), 1, 0)
    df['class_unchanged'] = np.where((df['prev_rank'] == df['curr_rank']), 1, 0)

    # --- Cleanup ---
    cols_to_drop = ['prev_actual_wt', 'prev_declared_wt', 'prev_rating', 
                    'prev_gear', 'prev_race_class', 'curr_rank', 'prev_rank']
    df.drop(columns=cols_to_drop, inplace=True)

    # --- Save ---
    print(f"Saving data to: {output_path}")
    df.to_csv(output_path, index=False)
    print("Done! Extended gear analysis complete.")

if __name__ == "__main__":
    main()