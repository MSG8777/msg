import pandas as pd
import numpy as np

# --- CONFIGURATION ---
VET_FILE = r"C:\HKJC_gemini_3_horse_racing_ML\hkjc_vet_db_REVERSE.csv"
# %%
RESULTS_FILE = r"C:\HKJC_gemini_3_horse_racing_ML\hkjc_race_results_v9_COURSES.csv"

OUTPUT_FILE = r"C:\HKJC_gemini_3_horse_racing_ML\HKJC_Master_Veterinary_Report.csv"

def extract_condition(text):
    """Parses raw text into standard medical categories and assigns severity."""
    text = str(text).lower()
    
    # Severity 5: Critical (Mandatory Bans)
    if 'heart' in text and ('irregular' in text or 'rhythm' in text or 'fibrillation' in text):
        return 'Heart Irregularity', 5
    if 'bled' in text or 'blood in trachea' in text:
        return 'Bleeding', 5
        
    # Severity 4: Orthopedic (Performance impacting)
    if 'lame' in text:
        return 'Lameness', 4
    if 'tendon' in text or 'ligament' in text or 'suspensory' in text:
        return 'Tendon/Ligament', 4
    if 'fracture' in text or 'bone' in text:
        return 'Fracture', 4
        
    # Severity 2: Illness/Minor
    if 'mucus' in text:
        return 'Mucus', 2
    if 'fever' in text or 'infection' in text:
        return 'Fever/Infection', 2
    if 'roarer' in text or 'epiglottic' in text:
        return 'Respiratory (Roarer)', 2
        
    # Severity 1: External/Mechanical
    if 'abrasion' in text or 'cut' in text or 'wound' in text or 'laceration' in text:
        return 'Cut/Wound', 1
    if 'cast' in text and 'barrier' in text:
        return 'Barrier Incident', 1
        
    return 'Other/Racing Incident', 0

print("1. Loading Data...")
df_vet = pd.read_csv(VET_FILE)
df_res = pd.read_csv(RESULTS_FILE)

# Normalize
df_vet['HorseName'] = df_vet['HorseName'].str.strip().str.upper()
df_res['HorseName'] = df_res['HorseName'].str.strip().str.upper()
df_vet['Date'] = pd.to_datetime(df_vet['Date'], format='%Y/%m/%d', errors='coerce')
df_res['Date'] = pd.to_datetime(df_res['Date'], format='%Y/%m/%d', errors='coerce')

# 2. Linking BrandNo (Crucial for merging databases)
print("2. Linking Brand Numbers...")
# Create a map of HorseName -> BrandNo
# (Merging on Name+Date is more accurate if names are reused)
brand_map = df_res[['Date', 'HorseName', 'BrandNo']].drop_duplicates()
df_master = pd.merge(df_vet, brand_map, on=['Date', 'HorseName'], how='left')

# 3. Categorizing Conditions
print("3. Categorizing Medical Issues...")
df_master[['Condition', 'Severity']] = df_master['Details'].apply(
    lambda x: pd.Series(extract_condition(x))
)

# 4. Calculating Recovery Time (Days Out)
print("4. Calculating Days Out...")
# Sort results for fast lookup
df_res_sorted = df_res[['BrandNo', 'Date']].sort_values(['BrandNo', 'Date']).drop_duplicates()
race_lookup = df_res_sorted.groupby('BrandNo')['Date'].apply(list).to_dict()

def calculate_next_run(row):
    brand = row['BrandNo']
    incident_date = row['Date']
    
    if pd.isna(brand) or brand not in race_lookup:
        return np.nan
        
    # Find all races after the incident
    future_races = [d for d in race_lookup[brand] if d > incident_date]
    
    if not future_races:
        return np.nan # Retired or hasn't run yet
        
    next_race = min(future_races)
    return (next_race - incident_date).days

df_master['Days_Out'] = df_master.apply(calculate_next_run, axis=1)

# Save
df_master.to_csv(OUTPUT_FILE, index=False)
print(f"Done! Saved Master Report to {OUTPUT_FILE}")
print(df_master[['Date', 'HorseName', 'Condition', 'Severity', 'Days_Out']].head(10))
