import pandas as pd
import numpy as np
import os
import re

# ==========================================
# CONFIGURATION
# ==========================================
# Ensure these match your actual folder structure
WORKING_DIR = r'C:\HKJC_gemini_3_horse_racing_ML\modify_csv'
RAW_DIR = r'C:\HKJC_gemini_3_horse_racing_ML\Finished csv'

# Input Files
FILE_VET_REVERSE = 'hkjc_vet_db_REVERSE.csv'
FILE_VET_MASTER = 'HKJC_Master_Veterinary_Report.csv'
FILE_PROFILES = 'scraped_profiles.csv'

# Reference File (For fixing IDs)
# This is the output from your previous "Main Pipeline"
FILE_MAIN_RESULTS = 'hkjc_race_results_MERGED_FINAL.csv' 

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def clean_profiles():
    """
    Standardizes the Horse Profile data.
    """
    print("\n=== Processing Profiles ===")
    path = os.path.join(RAW_DIR, FILE_PROFILES)
    if not os.path.exists(path): return
    
    df = pd.read_csv(path)
    
    # 1. Rename Cols
    rename_map = {
        'BrandNo': 'horse_code',
        'Origin': 'origin',
        'Colour': 'colour',
        'Sex': 'sex',
        'ImportType': 'import_type',
        'Sire': 'sire',
        'Dam': 'dam',
        'DamSire': 'dam_sire',
        'ImportDate': 'import_date'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # 2. Drop Redundant
    if 'RealBrandNo' in df.columns:
        df.drop(columns=['RealBrandNo'], inplace=True)
        
    # 3. Clean
    df['horse_code'] = df['horse_code'].str.strip().str.upper()
    
    out_path = os.path.join(WORKING_DIR, 'scraped_profiles_CLEANED.csv')
    df.to_csv(out_path, index=False)
    print(f"✓ Saved cleaned profiles to: {out_path}")

def clean_master_vet():
    """
    Standardizes the Master Vet Report (Injury History).
    """
    print("\n=== Processing Master Vet Report ===")
    path = os.path.join(RAW_DIR, FILE_VET_MASTER)
    if not os.path.exists(path): return

    df = pd.read_csv(path)
    
    # 1. Rename Cols
    rename_map = {
        'Date': 'date',
        'RaceNo': 'race_no',
        'HorseName': 'horse_name',
        'BrandNo': 'horse_code',
        'Details': 'vet_details',
        'Condition': 'vet_condition',
        'Severity': 'vet_severity',
        'Days_Out': 'days_out',
        'Source': 'data_source'
    }
    df.rename(columns=rename_map, inplace=True)
    
    # 2. Convert Date
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # 3. Fill Missing Horse Code (if possible, simple fill)
    # Note: Without a master map, we can't easily fill missing IDs here, 
    # but we can drop rows that are useless.
    df = df.dropna(subset=['horse_code', 'date'])
    
    out_path = os.path.join(WORKING_DIR, 'hkjc_master_vet_CLEANED.csv')
    df.to_csv(out_path, index=False)
    print(f"✓ Saved cleaned master vet report to: {out_path}")

def process_incident_report():
    """
    Advanced: Cleans the 'Reverse' Vet DB (Incident Report) and
    Features Engineers flags (Bumped, Checked, etc).
    Also attempts to attach Horse Code.
    """
    print("\n=== Processing Incident/Stipes Report (Reverse DB) ===")
    path = os.path.join(RAW_DIR, FILE_VET_REVERSE)
    if not os.path.exists(path): return

    df = pd.read_csv(path)
    
    # 1. Rename Cols
    df.rename(columns={
        'Date': 'date', 
        'RaceNo': 'race_no', 
        'HorseName': 'horse_name',
        'Details': 'incident_details',
        'Source': 'source'
    }, inplace=True)
    
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # 2. FEATURE ENGINEERING: Extract Keywords
    # This turns raw text into usable ML features
    print("   -> Extracting features from text...")
    df['incident_details'] = df['incident_details'].fillna('').astype(str).str.lower()
    
    df['inc_bumped'] = df['incident_details'].str.contains('bump', na=False).astype(int)
    df['inc_checked'] = df['incident_details'].str.contains('check', na=False).astype(int)
    df['inc_wide'] = df['incident_details'].str.contains('wide', na=False).astype(int)
    df['inc_block'] = df['incident_details'].str.contains('block|held up', na=False).astype(int)
    df['inc_blood'] = df['incident_details'].str.contains('blood|bleed', na=False).astype(int)

    # 3. FIX MISSING ID (Merge with Main Results)
    results_path = os.path.join(WORKING_DIR, FILE_MAIN_RESULTS)
    if os.path.exists(results_path):
        print("   -> Merging with Main Results to find Horse Codes...")
        df_res = pd.read_csv(results_path, usecols=['date', 'race_no', 'horse_name', 'horse_code'])
        df_res['date'] = pd.to_datetime(df_res['date'])
        
        # Merge
        df_merged = pd.merge(
            df, 
            df_res, 
            on=['date', 'race_no', 'horse_name'], 
            how='left'
        )
        
        # Check success rate
        missing = df_merged['horse_code'].isna().sum()
        total = len(df_merged)
        print(f"   -> Matched {total - missing}/{total} records to a Horse Code.")
        
        # Save
        out_path = os.path.join(WORKING_DIR, 'hkjc_incident_report_CLEANED.csv')
        df_merged.to_csv(out_path, index=False)
        print(f"✓ Saved incident report with IDs to: {out_path}")
    else:
        print("! Warning: Main Results file not found. Saving Incident Report without Horse Codes.")
        print("! (You won't be able to merge this easily later without Horse Codes)")
        out_path = os.path.join(WORKING_DIR, 'hkjc_incident_report_NO_ID.csv')
        df.to_csv(out_path, index=False)

# ==========================================
# EXECUTION
# ==========================================
if __name__ == "__main__":
    ensure_dir(WORKING_DIR)
    
    clean_profiles()
    clean_master_vet()
    process_incident_report()
    
    print("\nSupplementary Pipeline Complete.")
