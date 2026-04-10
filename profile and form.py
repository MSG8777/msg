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
INPUT_FILE = os.path.join(BASE_DIR, "Finished csv", "hkjc_race_results_v9_COURSES.csv") # Updated Input
PROFILE_FILE = os.path.join(BASE_DIR, "scraped_profiles.csv")
FORM_FILE = os.path.join(BASE_DIR, "scraped_form.csv")

MAX_WORKERS = 20      # Increased workers for probing
SAVE_INTERVAL = 20    # Save every 20 horses

# --- SESSION SETUP ---
def create_session():
    session = requests.Session()
    retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Connection": "keep-alive"
    })
    return session

# --- VINTAGE MAP (For Modern Era Optimization) ---
CYCLE_LETTERS = ['A', 'B', 'C', 'D', 'E', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'S', 'T', 'V']
ANCHOR_YEAR = 2016
VINTAGE_MAP = {}
for i, letter in enumerate(CYCLE_LETTERS):
    years = []
    # Map future and past cycles (2016, 2000, 1984...)
    for cycle in range(-4, 3):
        base_year = ANCHOR_YEAR + (cycle * 16)
        years.append(base_year + i)
    VINTAGE_MAP[letter] = sorted(years, reverse=True)

def get_real_brand_no(csv_brand_no):
    """
    Handles standard 'A123' and legacy 'AK123'/'BM123' formats.
    HKJC website expects 4 characters (Letter + 3 Digits).
    """
    s_bn = str(csv_brand_no).strip().upper()
    
    # Logic: If 5 chars and first 2 are letters, strip the first one.
    # e.g., 'BM035' -> 'M035', 'AK071' -> 'K071'
    if len(s_bn) == 5 and s_bn[0].isalpha() and s_bn[1].isalpha():
        return s_bn[1:]
    
    return s_bn

def get_probe_years(brand_no, min_race_year):
    """
    Returns a list of potential Vintage Years to try.
    Prioritizes VINTAGE_MAP match, then falls back to a [Year-3, Year+1] window.
    """
    real_bn = get_real_brand_no(brand_no)
    brand_letter = real_bn[0] if real_bn else ''
    
    candidates = []
    
    # 1. Try VINTAGE_MAP logic (Best for Modern Era)
    if brand_letter in VINTAGE_MAP:
        map_years = VINTAGE_MAP[brand_letter]
        # Find the map year closest to (MinRaceYear) but <= (MinRaceYear + 1)
        valid_map = [y for y in map_years if y <= (min_race_year + 1)]
        if valid_map:
            best_guess = max(valid_map)
            # Only use map if it's reasonably close (within 5 years)
            if abs(best_guess - min_race_year) < 5:
                candidates.append(best_guess)

    # 2. Add Probe Window (Best for Legacy Era / Unknown Patterns)
    # Most horses are imported 0-3 years before their first race.
    for y in range(min_race_year, min_race_year - 4, -1):
        candidates.append(y)
    
    # Deduplicate while preserving order
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
    profile_data = {}
    race_form = []
    
    # Check if page is valid (HKJC often returns 200 OK even for "System Message")
    if "System Message" in soup.get_text() or "No information" in soup.get_text():
        return {}, []

    # --- 1. PARSE PROFILE ---
    target_keys = {
        'Country of Origin': 'Origin',
        'Colour / Sex': 'ColourSex',
        'Import Type': 'ImportType',
        'Sire': 'Sire',
        'Dam': 'Dam',
        "Dam's Sire": 'DamSire',
        'Import Date': 'ImportDate'
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

    # --- 2. PARSE RACE FORM ---
    all_rows = soup.find_all('tr')
    for tr in all_rows:
        cols = [td.get_text(strip=True) for td in tr.find_all('td')]
        if len(cols) < 5: continue
        
        date_val = None
        date_idx = -1
        
        # Regex: d/m/yy or dd/mm/yy or yyyy
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
                    if cand.isdigit() or cand == '--':
                        rtg_val = cand
                
                gear_val = ""
                # Scan from end for Gear
                for x in reversed(cols):
                    if not x: continue
                    if "Video" in x: continue
                    if x.isdigit() and int(x) > 300: continue
                    if ':' in x: continue
                    gear_val = x
                    break
                
                if gear_val == '--': gear_val = ''
                if rtg_val == '--': rtg_val = ''
                
                race_form.append({
                    'Date': clean_date,
                    'Rtg': rtg_val,
                    'Gear': gear_val
                })
            except:
                continue

    return profile_data, race_form

def process_horse(horse_tuple):
    csv_brand_no, min_date = horse_tuple
    min_year = min_date.year
    
    # Determine probe order
    probe_years, real_brand_no = get_probe_years(csv_brand_no, min_year)
    
    result = {'profile': None, 'form': []}
    
    # Don't probe very old horses (Pre-1998) to save time, unless you really want to try.
    # Most pre-2000 pages are offline.
    # if min_year < 1998: return result 

    session = create_session()
    profile = {}
    form = []
    
    found = False
    
    # Endpoints to check
    # Try OtherHorse first for older horses, Horse for newer ones?
    # Actually checking both is safest, but costlier.
    # HKJC strategy: If horse is active -> Horse.aspx. If retired -> OtherHorse.aspx.
    endpoints = ['OtherHorse.aspx', 'Horse.aspx']
    if min_year >= 2023:
        endpoints = ['Horse.aspx', 'OtherHorse.aspx']

    try:
        for year in probe_years:
            if found: break
            horse_id = f"HK_{year}_{real_brand_no}"
            
            for endpoint in endpoints:
                url = f"https://racing.hkjc.com/racing/information/english/Horse/{endpoint}?HorseId={horse_id}&Option=1"
                try:
                    resp = session.get(url, timeout=5) # Short timeout for probes
                    if resp.status_code == 200:
                        p, f = parse_horse_page(resp.content)
                        # We accept the page if we found a Profile OR Form data
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
        # Prepare Profile
        profile_row = {
            'BrandNo': csv_brand_no,
            'RealBrandNo': real_brand_no, # Useful for debug
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
        
        # Prepare Form
        if form:
            for r in form:
                row = {'BrandNo': csv_brand_no} 
                row.update(r)
                result['form'].append(row)
            
    return result

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    print(f"Reading {INPUT_FILE}...")
    try:
        # Added low_memory=False to suppress mixed type warnings
        df = pd.read_csv(INPUT_FILE, low_memory=False) 
    except Exception as e:
        print(e)
        return

    # Updated: Explicitly tell pandas that the day comes first in the format, 
    # and handle slightly mixed string variations
    df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=True)
    
    horse_groups = df.groupby('BrandNo')['Date'].min()
    horse_list = [(bn, date) for bn, date in horse_groups.items()]
    total_horses = len(horse_list)
    
    print(f"Found {total_horses} unique horses (1979-Present).")
    print(f"Scraping to:\n  1. {PROFILE_FILE}\n  2. {FORM_FILE}")
    print(f"(Using {MAX_WORKERS} workers, saving every {SAVE_INTERVAL})...")
    
    # Initialize Files
    profile_cols = ['BrandNo', 'RealBrandNo', 'Origin', 'Colour', 'Sex', 'ImportType', 'Sire', 'Dam', 'DamSire', 'ImportDate']
    form_cols = ['BrandNo', 'Date', 'Rtg', 'Gear']
    
    pd.DataFrame(columns=profile_cols).to_csv(PROFILE_FILE, index=False)
    pd.DataFrame(columns=form_cols).to_csv(FORM_FILE, index=False)
    
    start_time = time.time()
    
    profile_buffer = []
    form_buffer = []
    completed_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_horse, h): h for h in horse_list}
        
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            completed_count += 1
            
            if res['profile']:
                profile_buffer.append(res['profile'])
            if res['form']:
                form_buffer.extend(res['form'])
            
            if completed_count % SAVE_INTERVAL == 0:
                elapsed = time.time() - start_time
                rate = completed_count / (elapsed if elapsed > 0 else 1)
                print(f"[{completed_count}/{total_horses}] Speed: {rate:.2f} horses/s. Found: {len(profile_buffer)} Profiles (Buffered)")

                # Save Profiles
                if profile_buffer:
                    df_p = pd.DataFrame(profile_buffer)
                    for c in profile_cols: 
                        if c not in df_p.columns: df_p[c] = ''
                    df_p[profile_cols].to_csv(PROFILE_FILE, mode='a', header=False, index=False)
                    profile_buffer = []

                # Save Form
                if form_buffer:
                    df_f = pd.DataFrame(form_buffer)
                    for c in form_cols:
                        if c not in df_f.columns: df_f[c] = ''
                    df_f[form_cols].to_csv(FORM_FILE, mode='a', header=False, index=False)
                    form_buffer = []
                
    # Final Save
    if profile_buffer:
        pd.DataFrame(profile_buffer)[profile_cols].to_csv(PROFILE_FILE, mode='a', header=False, index=False)
    if form_buffer:
        pd.DataFrame(form_buffer)[form_cols].to_csv(FORM_FILE, mode='a', header=False, index=False)
    
    print("Done.")

if __name__ == "__main__":
    main()
