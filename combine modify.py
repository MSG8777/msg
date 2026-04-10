import pandas as pd
import numpy as np
import re
import os
import shutil

# ==========================================
# CONFIGURATION
# ==========================================
INPUT_DIR = r'C:\HKJC_gemini_3_horse_racing_ML\Finished csv'
OUTPUT_DIR = r'C:\HKJC_gemini_3_horse_racing_ML\modify_csv'

# Constants
LENGTHS_PER_SECOND = 6.25

# File Names (Configuration)
FILES = {
    'results': 'hkjc_race_results_v9_COURSES.csv',
    'barrier': 'hkjc_barrier_trials_master_v7.csv',
    'sectional': 'hkjc_sectional_times_v13_FIXED.csv',
    'form': 'scraped_form.csv',
    'vet': 'HKJC_Master_Veterinary_Report.csv',
    'profile': 'scraped_profiles.csv'
}

# ==========================================
# UTILITY FUNCTIONS
# ==========================================
def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def unified_parse_lbw(val):
    """
    World Class Parser: Handles both Barrier and Sectional LBW formats.
    Converts 'Neck', 'Head', '2-1/4', '3L' etc. to float.
    """
    if pd.isna(val) or val == '' or val is None:
        return np.nan

    val_str = str(val).strip().upper().replace('L', '') # Remove 'L' for lengths

    # Racing Code Mappings (Unified)
    mapping = {
        'N': 0.3, 'NK': 0.3,      # Neck
        'SH': 0.1,                # Short Head
        'HD': 0.2, 'H': 0.2,      # Head
        'NOSE': 0.05,             # Nose
        'DH': 0.0,                # Dead Heat
        '-': 0.0, '0': 0.0,       # Leader / Zero
        '---': np.nan, 'WV': np.nan, 'PU': np.nan, 'DNF': np.nan,
        'UR': np.nan, 'FE': np.nan, 'DISQ': np.nan, 'NAN': np.nan
    }
    
    clean_code = val_str.lstrip('+-')
    if clean_code in mapping: return mapping[clean_code]
    if val_str in mapping: return mapping[val_str]

    try:
        # Fractions (e.g., "2-3/4" or "2 3/4")
        # Regex looks for Optional Integer + Separator + Numerator + / + Denominator
        match = re.match(r'^(\d+)?[-_\s]?(\d+)/(\d+)$', val_str)
        if match:
            i = int(match.group(1)) if match.group(1) else 0
            n = int(match.group(2))
            d = int(match.group(3))
            if d != 0: return i + (n / d)
        
        if val_str == '/4': return 0.25 # Edge case
        
        return float(val_str)
    except (ValueError, TypeError):
        return np.nan

# ==========================================
# CLASS: DATA PIPELINE
# ==========================================
class HKJCDataPipeline:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        ensure_dir(self.output_dir)
        
        # Schema Mapping (From csv standardize.py)
        self.schema_map = {
            FILES['results']: {
                "Date": "date", "RaceNo": "race_no", "Venue": "venue", "Class": "race_class",
                "Distance": "distance", "Going": "going", "Course": "course", "Place": "placing",
                "HorseNo": "horse_no", "HorseName": "horse_name", "BrandNo": "horse_code",
                "Jockey": "jockey", "Trainer": "trainer", "ActualWt": "actual_wt",
                "DeclarWt": "declared_wt", "Draw": "draw", "LBW": "lbw", "RunPos": "running_pos",
                "FinishTime": "finish_time", "WinOdds": "win_odds"
            },
            FILES['barrier']: {
                "Date": "date", "Batch": "batch_id", "Venue": "venue", "Surface": "surface",
                "Distance": "distance", "Going": "going", "BatchRawTime": "raw_time",
                "LeaderSectional": "leader_sectional", "Horse": "horse_name", "BrandNo": "horse_code",
                "Jockey": "jockey", "Trainer": "trainer", "Draw": "draw", "Gear": "gear",
                "LBW": "lbw", "RunningPosition": "running_pos", "FinishTime": "finish_time",
                "Result": "result"
            },
            FILES['sectional']: {
                "Date": "date", "RaceNo": "race_no", "HorseNo": "horse_no",
                "HorseName": "horse_name", "BrandNo": "horse_code", "FinishTime": "finish_time"
            },
            FILES['form']: {
                "BrandNo": "horse_code", "Date": "date", "Rtg": "rating", "Gear": "gear"
            }
        }

    def load_and_standardize(self, filename):
        """Loads CSV from Input, renames cols, saves to Output (intermediate)."""
        in_path = os.path.join(self.input_dir, filename)
        if not os.path.exists(in_path):
            print(f"Warning: File not found {in_path}")
            return None

        print(f"-> Loading & Standardizing: {filename}...")
        try:
            df = pd.read_csv(in_path, low_memory=False)
            
            # 1. Rename specific columns
            if filename in self.schema_map:
                df.rename(columns=self.schema_map[filename], inplace=True)
            
            # 2. General Snake Case
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
            
            return df
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            return None

    # ==========================================
    # STAGE 1: BARRIER TRIALS
    # ==========================================
    def process_barrier_trials(self):
        print("\n=== Processing Barrier Trials ===")
        df = self.load_and_standardize(FILES['barrier'])
        if df is None: return

        # 1. Filter Invalid Trials (No finish time)
        init_len = len(df)
        df = df[
            (df['finish_time'].notna()) & 
            (df['finish_time'] != '---') &
            (df['finish_time'].astype(str).str.strip() != '')
        ]
        print(f"   Dropped {init_len - len(df)} rows with invalid finish time.")

        # 2. Clean LBW
        if 'lbw' in df.columns:
            df['lbw'] = df['lbw'].apply(unified_parse_lbw)

        # 3. Drop Raw Time
        if 'raw_time' in df.columns:
            df.drop(columns=['raw_time'], inplace=True)

        # 4. Split Running Position & Extract Finish Pos
        if 'running_pos' in df.columns:
            # Extract last number as finish position
            def get_last_token(val):
                try:
                    tokens = str(val).strip().split()
                    return float(tokens[-1]) if tokens else None
                except: return None
            
            df['finish_pos'] = df['running_pos'].apply(get_last_token)

            # Split columns (running_pos_1, running_pos_2...)
            split_data = df['running_pos'].fillna('').astype(str).str.split(pat=' ', expand=True)
            for i in range(min(6, split_data.shape[1])):
                col_name = f'running_pos_{i+1}'
                df[col_name] = pd.to_numeric(split_data[i], errors='coerce')
            
            df.drop(columns=['running_pos'], inplace=True)

        # 5. Split Leader Sectional
        if 'leader_sectional' in df.columns:
            split_sec = df['leader_sectional'].fillna('').astype(str).str.split(pat=' ', expand=True)
            for i in range(min(6, split_sec.shape[1])):
                col_name = f'leader_sectional_{i+1}'
                df[col_name] = pd.to_numeric(split_sec[i], errors='coerce')
            df.drop(columns=['leader_sectional'], inplace=True)

        out_path = os.path.join(self.output_dir, FILES['barrier'].replace('.csv', '_CLEANED.csv'))
        df.to_csv(out_path, index=False)
        print(f"✓ Barrier Trials saved to: {out_path}")

    # ==========================================
    # STAGE 2: SECTIONAL TIMES
    # ==========================================
    def process_sectional_times(self):
        print("\n=== Processing Sectional Times ===")
        df = self.load_and_standardize(FILES['sectional'])
        if df is None: return

        # 1. Drop Invalid Finish Times
        df = df.dropna(subset=['finish_time'])
        df = df[df['finish_time'].astype(str).str.strip() != '']
        df = df[~df['finish_time'].astype(str).str.contains('---')]

        # --- FIX: Convert to datetime for correct sorting ---
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')

        df.sort_values(by=['date', 'race_no'], inplace=True)

        # 2. Parse LBW Columns
        lbw_cols = [c for c in df.columns if 'lbw' in c]
        for col in lbw_cols:
            df[col] = df[col].apply(unified_parse_lbw)

        # 3. Recalculate Positions & Fix LBW
        # Loop through sections 1 to 6
        for i in range(1, 7):
            time_cols = [f'sec{k}_time' for k in range(1, i+1)]
            pos_col = f'sec{i}_pos'
            lbw_col = f'sec{i}_lbw'
            
            if all(c in df.columns for c in time_cols):
                # Calc Cumulative Time
                cum_time = df[time_cols].sum(axis=1, min_count=len(time_cols))
                
                # Rank to get True Position
                new_pos = cum_time.groupby([df['date'], df['race_no']]).rank(method='min')
                df[pos_col] = new_pos
                
                # Estimate LBW based on time diff
                leader_time = cum_time.groupby([df['date'], df['race_no']]).transform('min')
                est_lbw = ((cum_time - leader_time) * LENGTHS_PER_SECOND).round(2)
                
                # Fill missing/invalid LBW with estimate
                if lbw_col not in df.columns:
                    df[lbw_col] = np.nan
                
                df[lbw_col] = df[lbw_col].fillna(est_lbw)
                mask_invalid = (df[lbw_col] < 0)
                df.loc[mask_invalid, lbw_col] = est_lbw[mask_invalid]
                
                # Force Leader LBW to 0
                df.loc[new_pos == 1, lbw_col] = 0.0

        out_path = os.path.join(self.output_dir, FILES['sectional'].replace('.csv', '_CLEANED.csv'))
        df.to_csv(out_path, index=False)
        print(f"✓ Sectional Times saved to: {out_path}")

    # ==========================================
    # STAGE 3: MERGE RESULT + FORM
    # ==========================================
    def process_race_results_merge(self):
        print("\n=== Processing Race Results & Form Merge ===")
        
        # Load datasets
        df_res = self.load_and_standardize(FILES['results'])
        df_form = self.load_and_standardize(FILES['form'])
        
        if df_res is None or df_form is None:
            print("Skipping merge due to missing files.")
            return

        # --- FIX: Added dayfirst=True to handle DD/MM/YYYY format ---
        # This was causing the ValueError on dates like 29/09/1979
        print("   Converting dates (DD/MM/YYYY)...")
        df_res['date'] = pd.to_datetime(df_res['date'], dayfirst=True, errors='coerce')
        df_form['date'] = pd.to_datetime(df_form['date'], dayfirst=True, errors='coerce')

        # Drop rows with invalid dates if any
        df_res = df_res.dropna(subset=['date'])
        df_form = df_form.dropna(subset=['date'])

        # Deduplicate Form
        df_form_clean = df_form.drop_duplicates(subset=['horse_code', 'date'])

        # Merge
        print("   Merging Results with Form...")
        merged_df = pd.merge(
            df_res,
            df_form_clean[['horse_code', 'date', 'rating', 'gear']],
            on=['horse_code', 'date'],
            how='left'
        )

        # Remove 'running_pos' (as requested in original script)
        if 'running_pos' in merged_df.columns:
            merged_df.drop(columns=['running_pos'], inplace=True)

        # Filter Non-Finishers (Code from 'remove unless row for result.py')
        non_finish_codes = [
            'WV', 'WV-A', 'WX', 'WX-A', 'WXNR', 
            'PU', 'FE', 'UR', 'DNF', 'TNP', 
            'VOID', 'DISQ', 'WR'
        ]
        
        print(f"   Filtering non-finishers (Rows before: {len(merged_df)})...")
        clean_df = merged_df[
            (~merged_df['placing'].isin(non_finish_codes)) & 
            (merged_df['placing'].notna()) & 
            (merged_df['finish_time'] != '---')
        ]
        print(f"   Rows after filtering: {len(clean_df)}")

        out_path = os.path.join(self.output_dir, 'hkjc_race_results_MERGED_FINAL.csv')
        clean_df.to_csv(out_path, index=False)
        print(f"✓ Final Merged Database saved to: {out_path}")

    # ==========================================
    # MAIN RUNNER
    # ==========================================
    def run_pipeline(self):
        print(f"Starting HKJC World Class Pipeline...")
        print(f"Input: {self.input_dir}")
        print(f"Output: {self.output_dir}")
        
        # Run Stages
        self.process_barrier_trials()
        self.process_sectional_times()
        self.process_race_results_merge()
        
        print("\nAll pipeline tasks completed successfully.")

# ==========================================
# EXECUTION
# ==========================================
if __name__ == "__main__":
    pipeline = HKJCDataPipeline(INPUT_DIR, OUTPUT_DIR)
    pipeline.run_pipeline()