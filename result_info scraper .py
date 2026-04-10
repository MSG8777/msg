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
LOG_FILE = os.path.join(OUTPUT_DIR, "scraping_log_v14.txt")

START_DATE = "1979/09/01"
END_DATE = "2026/01/15"
MAX_WORKERS = 40   

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

def log_debug(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} - {msg}\n")

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
    # date_str is YYYY/MM/DD
    url_date = date_str.replace("-", "/")
    
    # Validation Format: DD/MM/YYYY
    parts = url_date.split('/')
    validation_date_str = f"{parts[2]}/{parts[1]}/{parts[0]}" 

    base_url = f"https://racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx?RaceDate={url_date}&RaceNo=1"
    
    try:
        resp = session.get(base_url, timeout=15)
        
        # NOTE: If this validation is too strict for 1979 pages (different date format), 
        # it might cause low row counts. But we keep it as requested to ensure we are on the right page.
        if validation_date_str not in resp.text:
            return []

        if "Race 1" not in resp.text and "No information found" in resp.text:
            return []
            
    except:
        return []

    day_results = []
    
    # Iterate races
    for race_no in range(1, 15):
        race_url = f"https://racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx?RaceDate={url_date}&RaceNo={race_no}"
        
        try:
            resp = session.get(race_url, timeout=10)
            if resp.status_code != 200: break
            
            if validation_date_str not in resp.text:
                break

            soup = BeautifulSoup(resp.content, 'html.parser')
            meta_text_global = soup.get_text(" ", strip=True)

            # --- VENUE LOGIC UPDATED (First Occurrence Check) ---
            # 1. Find the index where each venue name appears.
            idx_st = meta_text_global.find("Sha Tin")
            idx_hv = meta_text_global.find("Happy Valley")
            
            venue = "UNK"
            
            # 2. Logic: The valid venue is in the header (early in text). 
            #    The "Next Meeting" venue is in the footer (late in text).
            if idx_st != -1 and idx_hv != -1:
                # Both found: Pick the one that appears FIRST.
                if idx_st < idx_hv:
                    venue = "ST"
                else:
                    venue = "HV"
            elif idx_st != -1:
                venue = "ST"
            elif idx_hv != -1:
                venue = "HV"
            # ----------------------------------------------------

            # 2. HEADER CONTEXT
            full_text = soup.get_text("\n")
            lines = [l.strip() for l in full_text.split('\n') if l.strip()]
            race_header = ""
            start_idx = -1
            race_pat = re.compile(rf'^Race\s+{race_no}\s+\(', re.IGNORECASE)
            
            for i, line in enumerate(lines):
                if race_pat.search(line):
                    start_idx = i
                    break
            
            if start_idx != -1:
                race_header = " ".join(lines[start_idx:start_idx+15])
            else:
                race_header = meta_text_global

            race_header = re.sub(r'\s+', ' ', race_header)
            
            # 3. METADATA
            dist_match = re.search(r'(\d{4})M', race_header)
            distance = dist_match.group(1) if dist_match else "0"
            
            # CLASS LOGIC
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
            
            # GOING
            going_match = re.search(r'Going\s*:\s*(\w+)', race_header)
            if not going_match: going_match = re.search(r'Going\s*:\s*(\w+)', meta_text_global)
            going = going_match.group(1) if going_match else ""

            # COURSE
            if "All Weather Track" in race_header: course = "AWT"
            else:
                c_match = re.search(r'Course\s*:\s*Turf\s*-\s*["\']?([A-Z0-9\+]+)["\']?\s*Course', race_header, re.IGNORECASE)
                if not c_match: c_match = re.search(r'Turf\s*-\s*["\']?([A-Z0-9\+]+)["\']?\s*Course', race_header, re.IGNORECASE)
                course = "Turf-" + c_match.group(1) if c_match else ("Turf" if "Turf" in race_header else "UNK")

            # TABLE PARSING
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
                    "Date": date_str,
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

# --- WORKER ---
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

# --- MAIN ---
def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    
    headers = ["Date", "RaceNo", "Venue", "Class", "Distance", "Going", "Course", 
               "Place", "HorseNo", "HorseName", "BrandNo", "Jockey", "Trainer", 
               "ActualWt", "DeclarWt", "Draw", "LBW", "RunPos", "FinishTime", "WinOdds"]
    
    # Overwrite mode
    if os.path.exists(DATA_FILE): os.remove(DATA_FILE)
    pd.DataFrame(columns=headers).to_csv(DATA_FILE, index=False)
    
    start = datetime.strptime(START_DATE, "%Y/%m/%d")
    end = datetime.strptime(END_DATE, "%Y/%m/%d")
    
    date_list = []
    curr = start
    while curr <= end:
        date_list.append(curr)
        curr += timedelta(days=1)
    
    print(f"--- HKJC SCRAPER v14 (VENUE FIXED: FIRST OCCURRENCE) ---")
    print(f"Target: {START_DATE} to {END_DATE}")
    print(f"Output: {DATA_FILE}\n")

    batch_data = []
    total_saved = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_date = {executor.submit(worker_task, d): d for d in date_list}
        
        for i, future in enumerate(as_completed(future_to_date)):
            data = future.result()
            if data:
                batch_data.extend(data)
            
            if i % 10 == 0: 
                progress = (i / len(date_list)) * 100
                sys.stdout.write(f"\rProgress: {i}/{len(date_list)} ({progress:.1f}%) | Found: {total_saved + len(batch_data)} rows")
                sys.stdout.flush()

            if len(batch_data) >= 200:
                df = pd.DataFrame(batch_data)
                df.to_csv(DATA_FILE, mode='a', header=False, index=False)
                total_saved += len(batch_data)
                batch_data = []
                gc.collect() 

    if batch_data:
        df = pd.DataFrame(batch_data)
        df.to_csv(DATA_FILE, mode='a', header=False, index=False)
        print(f"\nDone. Total rows saved: {total_saved + len(batch_data)}")
    else:
        print(f"\nDone. Total rows saved: {total_saved}")

if __name__ == "__main__":
    main()