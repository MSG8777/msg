import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
import re
import concurrent.futures
from requests.adapters import HTTPAdapter, Retry

# --- CONFIGURATION ---
BASE_DIR = r"C:\HKJC_gemini_3_horse_racing_ML"

# Input Files (Sources of Truth for BrandNos)
RESULTS_FILE = os.path.join(BASE_DIR, "Finished csv", "hkjc_race_results_v9_COURSES.csv")
TRIALS_FILE = os.path.join(BASE_DIR, "Finished csv", "hkjc_barrier_trials_master_v7.csv")

# Database Files (Files to Update)
PROFILE_FILE = os.path.join(BASE_DIR, "scraped_profiles.csv")
FORM_FILE = os.path.join(BASE_DIR, "scraped_form.csv")

MAX_WORKERS = 20      # High concurrency for speed
SAVE_INTERVAL = 20    # Save batch size

# --- SESSION SETUP ---
def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Connection": "keep-alive"
    })
    return session

# --- VINTAGE MAP ---
CYCLE_LETTERS = ['A', 'B', 'C', 'D', 'E', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'S', 'T', 'V']
ANCHOR_YEAR = 2016
VINTAGE_MAP = {}
for i, letter in enumerate(CYCLE_LETTERS):
    years = []
    for cycle in range(-4, 3):
        base_year = ANCHOR_YEAR + (cycle * 16)
        years.append(base_year + i)
    VINTAGE_MAP[letter] = sorted(years, reverse=True)

def get_real_brand_no(csv_brand_no):
    s_bn = str(csv_brand_no).strip().upper()
    if len(s_bn) == 5 and s_bn[0].isalpha() and s_bn[1].isalpha():
        return s_bn[1:]
    return s_bn

def get_probe_years(brand_no, min_race_year):
    real_bn = get_real_brand_no(brand_no)
    brand_letter = real_bn[0] if real_bn else ''
    candidates = []
    
    # 1. Map Logic
    if brand_letter in VINTAGE_MAP:
        map_years = VINTAGE_MAP[brand_letter]
        valid_map = [y for y in map_years if y <= (min_race_year + 1)]
        if valid_map:
            best_guess = max(valid_map)
            if abs(best_guess - min_race_year) < 5:
                candidates.append(best_guess)

    # 2. Window Logic (Fallback)
    for y in range(min_race_year, min_race_year - 4, -1):
        candidates.append(y)
    
    seen = set()
    final_list = []
    for c in candidates:
        if c not in seen:
            final_list.append(c)
            seen.add(c)
    return final_list, real_bn

def parse_horse_page(html_content):
    if not html_content: return {}, []
    soup = BeautifulSoup(html_content, 'html.parser')
    
    if "System Message" in soup.get_text() or "No information" in soup.get_text():
        return {}, []

    # Parse Profile
    profile_data = {}
    target_keys = {
        'Country of Origin': 'Origin', 'Colour / Sex': 'ColourSex',
        'Import Type': 'ImportType', 'Sire': 'Sire', 'Dam': 'Dam',
        "Dam's Sire": 'DamSire', 'Import Date': 'ImportDate'
    }
    
    all_cells = soup.find_all('td')
    cells_text = [c.get_text(" ", strip=True) for c in all_cells if not c.find('table')]
    
    found_keys = set()
    for i, text in enumerate(cells_text):
        for label, key in target_keys.items():
            if key in found_keys: continue
            if label == 'Sire' and "Dam's Sire" in text: continue
            if label == 'Dam' and "Dam's Sire" in text: continue
            
            if label in text:
                val = ""
                if ':' in text:
                    parts = text.split(':', 1)
                    if label in parts[0]: val = parts[1].strip()
                
                if not val and i + 1 < len(cells_text):
                    next_text = cells_text[i+1]
                    if next_text == ':' and i + 2 < len(cells_text):
                        val = cells_text[i+2]
                    elif next_text and ':' not in next_text:
                        val = next_text

                if val:
                    if key == 'Origin':
                        if '/' in val: val = val.split('/')[0].strip()
                        profile_data['Origin'] = val
                    elif key == 'ColourSex':
                        if '/' in val:
                            parts = val.split('/')
                            profile_data['Colour'] = parts[0].strip()
                            profile_data['Sex'] = parts[1].strip() if len(parts)>1 else ''
                        else:
                            profile_data['Colour'] = val
                            profile_data['Sex'] = ''
                    else:
                        profile_data[key] = val
                    found_keys.add(key)
                    break

    # Parse Form
    race_form = []
    all_rows = soup.find_all('tr')
    for tr in all_rows:
        cols = [td.get_text(strip=True) for td in tr.find_all('td')]
        if len(cols) < 5: continue
        
        date_val = None
        date_idx = -1
        for i, c in enumerate(cols[:5]): 
            if re.match(r'^\d{1,2}/\d{1,2}/\d{2,4}$', c): 
                date_val = c
                date_idx = i
                break
        
        if date_val:
            try:
                parts = date_val.split('/')
                y = parts[2]
                if len(y) == 2: y = '20' + y
                m = parts[1].zfill(2)
                d = parts[0].zfill(2)
                clean_date = f"{y}-{m}-{d}"
                
                rtg_val = ""
                rtg_idx = date_idx + 6
                if rtg_idx < len(cols):
                    cand = cols[rtg_idx]
                    if cand.isdigit() or cand == '--': rtg_val = cand
                
                gear_val = ""
                for x in reversed(cols):
                    if not x: continue
                    if "Video" in x or (x.isdigit() and int(x) > 300) or ':' in x: continue
                    gear_val = x
                    break
                
                if gear_val == '--': gear_val = ''
                if rtg_val == '--': rtg_val = ''
                
                race_form.append({'Date': clean_date, 'Rtg': rtg_val, 'Gear': gear_val})
            except: continue

    return profile_data, race_form

def process_horse(horse_tuple):
    csv_brand_no, min_date = horse_tuple
    
    # Handle NaN dates (default to current year if unknown)
    if pd.isna(min_date):
        min_year = 2024
    else:
        min_year = min_date.year
    
    probe_years, real_brand_no = get_probe_years(csv_brand_no, min_year)
    result = {'profile': None, 'form': []}

    session = create_session()
    profile = {}
    form = []
    found = False
    
    # Check Active then Retired
    endpoints = ['Horse.aspx', 'OtherHorse.aspx']
    if min_year < 2022: # Older horses likely in OtherHorse
        endpoints = ['OtherHorse.aspx', 'Horse.aspx']

    try:
        for year in probe_years:
            if found: break
            horse_id = f"HK_{year}_{real_brand_no}"
            
            for endpoint in endpoints:
                url = f"https://racing.hkjc.com/racing/information/english/Horse/{endpoint}?HorseId={horse_id}&Option=1"
                try:
                    resp = session.get(url, timeout=5)
                    if resp.status_code == 200:
                        p, f = parse_horse_page(resp.content)
                        if p or f:
                            profile = p
                            form = f
                            found = True
                            break
                except: pass
                if found: break
    finally:
        session.close()

    if found:
        profile_row = {
            'BrandNo': csv_brand_no,
            'RealBrandNo': real_brand_no,
            'Origin': profile.get('Origin', ''),
            'Colour': profile.get('Colour', ''),
            'Sex': profile.get('Sex', ''),
            'ImportType': profile.get('ImportType', ''),
            'Sire': profile.get('Sire', ''),
            'Dam': profile.get('Dam', ''),
            'DamSire': profile.get('DamSire', ''),
            'ImportDate': profile.get('ImportDate', '')
        }
        result['profile'] = profile_row
        
        for r in form:
            row = {'BrandNo': csv_brand_no} 
            row.update(r)
            result['form'].append(row)
            
    return result

def get_missing_horses():
    print("--- IDENTIFYING MISSING HORSES ---")
    
    # 1. Load Existing Profiles
    if os.path.exists(PROFILE_FILE):
        try:
            existing_df = pd.read_csv(PROFILE_FILE)
            # Ensure BrandNo is string
            existing_df['BrandNo'] = existing_df['BrandNo'].astype(str).str.strip()
            existing_brands = set(existing_df['BrandNo'].unique())
            print(f"Existing Profiles: {len(existing_brands)}")
        except Exception as e:
            print(f"Error reading profile file: {e}")
            existing_brands = set()
    else:
        print("No existing profile file found.")
        existing_brands = set()

    # 2. Load Race Results
    horses_data = []
    
    print(f"Reading Race Results: {RESULTS_FILE}")
    if os.path.exists(RESULTS_FILE):
        df_res = pd.read_csv(RESULTS_FILE, usecols=['BrandNo', 'Date'])
        df_res['Date'] = pd.to_datetime(df_res['Date'], errors='coerce')
        # Drop invalid BrandNos
        df_res = df_res.dropna(subset=['BrandNo'])
        df_res['BrandNo'] = df_res['BrandNo'].astype(str).str.strip()
        horses_data.append(df_res)
    else:
        print(f"Warning: Results file not found at {RESULTS_FILE}")

    # 3. Load Barrier Trials
    print(f"Reading Barrier Trials: {TRIALS_FILE}")
    if os.path.exists(TRIALS_FILE):
        df_trials = pd.read_csv(TRIALS_FILE, usecols=['BrandNo', 'Date'])
        df_trials['Date'] = pd.to_datetime(df_trials['Date'], errors='coerce')
        df_trials = df_trials.dropna(subset=['BrandNo'])
        df_trials['BrandNo'] = df_trials['BrandNo'].astype(str).str.strip()
        horses_data.append(df_trials)
    else:
        print(f"Warning: Trials file not found at {TRIALS_FILE}")

    if not horses_data:
        print("No source data found!")
        return []

    # 4. Combine and Find Min Date
    full_df = pd.concat(horses_data)
    
    # Group by BrandNo to get earliest date
    # This helps the scraper know which year to probe
    horse_stats = full_df.groupby('BrandNo')['Date'].min().reset_index()
    
    # Filter out horses we already have
    missing_mask = ~horse_stats['BrandNo'].isin(existing_brands)
    missing_horses_df = horse_stats[missing_mask]
    
    # Convert to list of tuples: (BrandNo, MinDate)
    missing_list = list(zip(missing_horses_df['BrandNo'], missing_horses_df['Date']))
    
    print(f"Total Unique Horses in Data: {len(horse_stats)}")
    print(f"Missing Horses to Scrape: {len(missing_list)}")
    
    return missing_list

def main():
    # Identify work to be done
    missing_list = get_missing_horses()
    
    if not missing_list:
        print("Database is up to date! No missing horses found.")
        return

    print(f"\nStarting scrape for {len(missing_list)} horses...")
    print(f"Using {MAX_WORKERS} workers...")

    # Define Columns
    profile_cols = ['BrandNo', 'RealBrandNo', 'Origin', 'Colour', 'Sex', 'ImportType', 'Sire', 'Dam', 'DamSire', 'ImportDate']
    form_cols = ['BrandNo', 'Date', 'Rtg', 'Gear']

    # Ensure files exist with headers if they don't
    if not os.path.exists(PROFILE_FILE):
        pd.DataFrame(columns=profile_cols).to_csv(PROFILE_FILE, index=False)
    if not os.path.exists(FORM_FILE):
        pd.DataFrame(columns=form_cols).to_csv(FORM_FILE, index=False)

    start_time = time.time()
    profile_buffer = []
    form_buffer = []
    completed_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_horse, h): h for h in missing_list}
        
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            completed_count += 1
            
            if res['profile']:
                profile_buffer.append(res['profile'])
            if res['form']:
                form_buffer.extend(res['form'])
            
            # Batch Save
            if completed_count % SAVE_INTERVAL == 0 or completed_count == len(missing_list):
                elapsed = time.time() - start_time
                rate = completed_count / (elapsed if elapsed > 0 else 1)
                print(f"[{completed_count}/{len(missing_list)}] Speed: {rate:.2f} horses/s. New Profiles: {len(profile_buffer)}")

                if profile_buffer:
                    df_p = pd.DataFrame(profile_buffer)
                    # Ensure all columns exist
                    for c in profile_cols:
                        if c not in df_p.columns: df_p[c] = ''
                    # Append mode
                    df_p[profile_cols].to_csv(PROFILE_FILE, mode='a', header=False, index=False)
                    profile_buffer = []

                if form_buffer:
                    df_f = pd.DataFrame(form_buffer)
                    for c in form_cols:
                        if c not in df_f.columns: df_f[c] = ''
                    # Append mode
                    df_f[form_cols].to_csv(FORM_FILE, mode='a', header=False, index=False)
                    form_buffer = []

    print("\nUpdate Complete.")

if __name__ == "__main__":
    main()