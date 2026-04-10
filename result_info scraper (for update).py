import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import time
import random
import re
import sys
import gc
from io import StringIO
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter, Retry

# --- CONFIGURATION ---
OUTPUT_DIR = r"C:\HKJC_gemini_3_horse_racing_ML"
DATA_FILE = os.path.join(OUTPUT_DIR, "hkjc_race_results_v9_COURSES.csv")
# Default start if file doesn't exist
DEFAULT_START_DATE = "1979/09/01" 
# End date is set to "Today"
END_DATE = datetime.now().strftime("%Y/%m/%d") 
MAX_WORKERS = 20 

# --- SESSION SETUP ---
def create_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Connection": "keep-alive"
    })
    return session

# --- CONVERSION HELPER ---
def clean_lbw(val):
    if val is None: return None
    val = str(val).strip().upper()
    if val == '-' or val == '0': return 0.0
    if val == '---' or val == '' or val == 'NAN': return None
    if val == 'NOSE': return 0.05
    if val == 'SH':   return 0.10
    if val == 'HD':   return 0.20
    if val == 'N':    return 0.25
    if val == 'NK':   return 0.25
    try:
        if '-' in val:
            parts = val.split('-')
            whole = float(parts[0])
            if '/' in parts[1]:
                frac_parts = parts[1].split('/')
                fraction = float(frac_parts[0]) / float(frac_parts[1])
                return whole + fraction
            else:
                return float(val)
        elif '/' in val:
            frac_parts = val.split('/')
            return float(frac_parts[0]) / float(frac_parts[1])
        else:
            return float(val)
    except:
        return None

# --- PARSING LOGIC ---
def parse_race_day(session, date_str):
    # date_str input is always YYYY/MM/DD from the worker loop
    url_date = date_str.replace("-", "/")
    parts = url_date.split('/')
    
    # 1. Validation Date: DD/MM/YYYY (Matches HKJC HTML text)
    validation_date_str = f"{parts[2]}/{parts[1]}/{parts[0]}" 
    
    # 2. OUTPUT DATE: DD/MM/YYYY (Strictly matches your existing CSV)
    csv_output_date = f"{parts[2]}/{parts[1]}/{parts[0]}"

    base_url = f"https://racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx?RaceDate={url_date}&RaceNo=1"
    
    try:
        resp = session.get(base_url, timeout=15)
        if validation_date_str not in resp.text: return []
        if "Race 1" not in resp.text and "No information found" in resp.text: return []
    except:
        return []

    day_results = []
    
    for race_no in range(1, 15):
        race_url = f"https://racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx?RaceDate={url_date}&RaceNo={race_no}"
        
        try:
            resp = session.get(race_url, timeout=10)
            if resp.status_code != 200: break
            if validation_date_str not in resp.text: break

            soup = BeautifulSoup(resp.content, 'html.parser')
            meta_text_global = soup.get_text(" ", strip=True)

            # Venue Logic
            idx_st = meta_text_global.find("Sha Tin")
            idx_hv = meta_text_global.find("Happy Valley")
            venue = "UNK"
            if idx_st != -1 and idx_hv != -1:
                venue = "ST" if idx_st < idx_hv else "HV"
            elif idx_st != -1: venue = "ST"
            elif idx_hv != -1: venue = "HV"

            # Header Parsing
            full_text = soup.get_text("\n")
            lines = [l.strip() for l in full_text.split('\n') if l.strip()]
            race_header = ""
            start_idx = -1
            race_pat = re.compile(rf'^Race\s+{race_no}\s+\(', re.IGNORECASE)
            
            for i, line in enumerate(lines):
                if race_pat.search(line):
                    start_idx = i
                    break
            
            # --- GHOST RACE FIX ---
            # If header is missing, stop processing this day.
            if start_idx != -1:
                race_header = " ".join(lines[start_idx:start_idx+15])
            else:
                break 

            race_header = re.sub(r'\s+', ' ', race_header)
            
            # Metadata
            dist_match = re.search(r'(\d{4})M', race_header)
            distance = dist_match.group(1) if dist_match else "0"
            
            if re.search(r'Group\s+(1|One|I)', race_header, re.IGNORECASE): race_class = "Group 1"
            elif re.search(r'Group\s+(2|Two|II)', race_header, re.IGNORECASE): race_class = "Group 2"
            elif re.search(r'Group\s+(3|Three|III)', race_header, re.IGNORECASE): race_class = "Group 3"
            elif re.search(r'Listed', race_header, re.IGNORECASE): race_class = "Listed"
            elif re.search(r'(Derby|Classic\s*Mile|Classic\s*Cup|4\s*Year\s*Old)', race_header, re.IGNORECASE): race_class = "4 Year Old"
            elif re.search(r'Griffin', race_header, re.IGNORECASE): race_class = "Griffin"
            elif re.search(r'Restricted', race_header, re.IGNORECASE):
                class_match = re.search(r'Class\s+(\d)', race_header, re.IGNORECASE)
                race_class = f"Class {class_match.group(1)} (Restricted)" if class_match else "Restricted"
            else:
                class_match = re.search(r'Class\s+(\d)', race_header, re.IGNORECASE)
                race_class = f"Class {class_match.group(1)}" if class_match else "Open"
            
            going_match = re.search(r'Going\s*:\s*(\w+)', race_header)
            if not going_match: going_match = re.search(r'Going\s*:\s*(\w+)', meta_text_global)
            going = going_match.group(1) if going_match else ""

            if "All Weather Track" in race_header: course = "AWT"
            else:
                c_match = re.search(r'Course\s*:\s*Turf\s*-\s*["\']?([A-Z0-9\+]+)["\']?\s*Course', race_header, re.IGNORECASE)
                if not c_match: c_match = re.search(r'Turf\s*-\s*["\']?([A-Z0-9\+]+)["\']?\s*Course', race_header, re.IGNORECASE)
                course = "Turf-" + c_match.group(1) if c_match else ("Turf" if "Turf" in race_header else "UNK")

            # Table Parsing
            dfs = pd.read_html(StringIO(str(soup)))
            target_df = None
            for df in dfs:
                headers = [str(c).lower() for c in df.columns]
                if any('jockey' in h for h in headers) and any('horse' in h for h in headers):
                    target_df = df
                    break
            
            if target_df is None: continue

            if isinstance(target_df.columns, pd.MultiIndex):
                target_df.columns = [' '.join(col).strip() for col in target_df.columns.values]

            target_df.columns = [str(c).strip() for c in target_df.columns]
            col_map = {}
            for col in target_df.columns:
                c_low = col.lower()
                if 'declar' in c_low: col_map[col] = 'DeclarWt'
                elif 'act' in c_low and 'wt' in c_low: col_map[col] = 'ActualWt'
                elif 'place' in c_low or 'plc' in c_low: col_map[col] = 'Place'
                elif 'no.' in c_low: col_map[col] = 'HorseNo'
                elif 'jockey' in c_low: col_map[col] = 'Jockey'
                elif 'trainer' in c_low: col_map[col] = 'Trainer'
                elif 'draw' in c_low or 'dr.' in c_low: col_map[col] = 'Draw'
                elif 'lbw' in c_low: col_map[col] = 'LBW'
                elif 'running' in c_low or 'pos' in c_low: col_map[col] = 'RunPos'
                elif 'finish' in c_low or 'time' in c_low: col_map[col] = 'FinishTime'
                elif 'odds' in c_low: col_map[col] = 'WinOdds'
                elif 'horse' in c_low and 'wt' not in c_low: col_map[col] = 'HorseName'

            target_df = target_df.rename(columns=col_map)
            target_df = target_df.loc[:, ~target_df.columns.duplicated()]
            if 'Place' not in target_df.columns and len(target_df.columns) > 0:
                target_df = target_df.rename(columns={target_df.columns[0]: 'Place'})

            if 'HorseName' not in target_df.columns: continue

            for _, row in target_df.iterrows():
                def get_clean_val(key):
                    if key not in row: return ""
                    val = row[key]
                    if isinstance(val, pd.Series): val = val.iloc[0]
                    if pd.isna(val): return ""
                    return str(val).strip()

                place_val = get_clean_val('Place')
                if place_val.lower() == 'place': continue

                h_raw = get_clean_val('HorseName')
                if '(' in h_raw:
                    parts = h_raw.split('(')
                    name_part = parts[0].strip()
                    brand_part = parts[1].replace(')', '').strip()
                    if len(brand_part) > 6: brand_part = brand_part[:4]
                else:
                    name_part = h_raw
                    brand_part = ""

                day_results.append({
                    "Date": csv_output_date, # STROING AS DD/MM/YYYY
                    "RaceNo": race_no,
                    "Venue": venue,
                    "Class": race_class,
                    "Distance": distance,
                    "Going": going,
                    "Course": course,
                    "Place": place_val,
                    "HorseNo": get_clean_val('HorseNo'),
                    "HorseName": name_part,
                    "BrandNo": brand_part,
                    "Jockey": get_clean_val('Jockey'),
                    "Trainer": get_clean_val('Trainer'),
                    "ActualWt": get_clean_val('ActualWt'),
                    "DeclarWt": get_clean_val('DeclarWt'),
                    "Draw": get_clean_val('Draw'),
                    "LBW": clean_lbw(get_clean_val('LBW')),
                    "RunPos": get_clean_val('RunPos'),
                    "FinishTime": get_clean_val('FinishTime'),
                    "WinOdds": get_clean_val('WinOdds')
                })

        except Exception as e:
            continue

    return day_results

def worker_task(date_obj):
    session = create_session()
    date_str = date_obj.strftime("%Y/%m/%d")
    
    # Summer Break Skip
    if (date_obj.month == 7 and date_obj.day > 16) or (date_obj.month == 8):
        session.close()
        return None

    try:
        time.sleep(random.uniform(0.1, 0.3))
        data = parse_race_day(session, date_str)
    except:
        data = []
    finally:
        session.close()
        
    return data

# --- UPDATER INTELLIGENCE (Strict DD/MM/YYYY Read) ---
def get_start_date_from_db():
    if not os.path.exists(DATA_FILE):
        print(f"Database not found at {DATA_FILE}. Starting from scratch ({DEFAULT_START_DATE}).")
        return DEFAULT_START_DATE
    
    try:
        header_df = pd.read_csv(DATA_FILE, nrows=0)
        date_col_name = next((col for col in header_df.columns if 'date' in col.lower()), None)

        if not date_col_name:
            print("ERROR: Could not find a 'Date' column in the CSV. Starting from scratch.")
            return DEFAULT_START_DATE

        df = pd.read_csv(DATA_FILE, usecols=[date_col_name])
        
        if df.empty:
            print("Database is empty. Starting from scratch.")
            return DEFAULT_START_DATE
            
        # FIX: dayfirst=True ensures 11/01/2026 is read as Jan 11 (Past), not Nov 1 (Future)
        df['parsed_date'] = pd.to_datetime(df[date_col_name], errors='coerce', dayfirst=True)
        
        last_date = df['parsed_date'].max()
        
        if pd.isna(last_date):
            print("ERROR: Could not parse any valid dates from the file. Starting from scratch.")
            return DEFAULT_START_DATE
            
        print(f"Last race found in DB: {last_date.strftime('%Y-%m-%d')}")
        
        new_start = last_date + timedelta(days=1)
        return new_start.strftime("%Y/%m/%d")
        
    except Exception as e:
        print(f"CRITICAL ERROR reading DB: {e}")
        print("Starting from scratch as a fallback.")
        return DEFAULT_START_DATE

# --- MAIN ---
def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    
    print("--- HKJC INCREMENTAL UPDATER v6 (Consistent DD/MM/YYYY) ---")
    
    start_date_str = get_start_date_from_db()
    
    start = datetime.strptime(start_date_str, "%Y/%m/%d")
    end = datetime.strptime(END_DATE, "%Y/%m/%d")
    
    if start > end:
        print(f"Database is already up to date.")
        print(f"Current Date: {END_DATE}")
        return

    print(f"Updating Database: {DATA_FILE}")
    print(f"Fetching Range: {start_date_str} to {END_DATE}")
    
    headers = ["Date", "RaceNo", "Venue", "Class", "Distance", "Going", "Course", 
               "Place", "HorseNo", "HorseName", "BrandNo", "Jockey", "Trainer", 
               "ActualWt", "DeclarWt", "Draw", "LBW", "RunPos", "FinishTime", "WinOdds"]
               
    if not os.path.exists(DATA_FILE):
        pd.DataFrame(columns=headers).to_csv(DATA_FILE, index=False)

    date_list = []
    curr = start
    while curr <= end:
        date_list.append(curr)
        curr += timedelta(days=1)
    
    batch_data = []
    total_saved = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_date = {executor.submit(worker_task, d): d for d in date_list}
        
        for i, future in enumerate(as_completed(future_to_date)):
            data = future.result()
            if data:
                batch_data.extend(data)
            
            progress = ((i + 1) / len(date_list)) * 100
            sys.stdout.write(f"\rProgress: {i+1}/{len(date_list)} ({progress:.1f}%) | Found New Rows: {total_saved + len(batch_data)}")
            sys.stdout.flush()

            if len(batch_data) >= 50:
                df = pd.DataFrame(batch_data)
                df.to_csv(DATA_FILE, mode='a', header=False, index=False)
                total_saved += len(batch_data)
                batch_data = []
                gc.collect() 

    if batch_data:
        df = pd.DataFrame(batch_data)
        df.to_csv(DATA_FILE, mode='a', header=False, index=False)
        total_saved += len(batch_data)

    print(f"\nUpdate Complete. Total new rows added: {total_saved}")

if __name__ == "__main__":
    main()