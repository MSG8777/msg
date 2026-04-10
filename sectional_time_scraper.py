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
DATA_FILE = os.path.join(OUTPUT_DIR, "hkjc_sectional_times_v13_FIXED.csv")
LOG_FILE = os.path.join(OUTPUT_DIR, "sectional_log_v13.txt")

# Date Range
START_DATE = "2008/04/02"
END_DATE = "2025/12/24"
MAX_WORKERS = 8

# --- PARSING HELPER ---
def split_sectional_data(val):
    """
    Splits clumped string like '1218-1/4 26.04' into ('12', '18-1/4', '26.04')
    """
    if pd.isna(val) or val == "":
        return None, None, None
    val = str(val).strip()
    
    # 1. FIND TIME: Look for the first float (xx.xx)
    times = re.findall(r'\d+\.\d{2}', val)
    if not times:
        return None, None, None
    
    section_time = times[0]
    
    # Split string by the time to get the prefix (Pos + Margin)
    parts = val.split(section_time)
    prefix = parts[0].strip()
    
    if not prefix:
        return None, None, section_time

    # 2. SPLIT POSITION AND MARGIN
    # Logic: Position is always 1-14. 
    match = re.match(r'^(\d{1,2})(.*)', prefix)
    
    pos = None
    margin = None
    
    if match:
        p_str = match.group(1)
        m_str = match.group(2)
        
        # Ambiguity Check: "114-1/4" -> Pos 11, Margin 4-1/4
        # Ambiguity Check: "31/2" -> Pos 3, Margin 1/2
        if int(p_str) > 14:
            pos = p_str[0]
            margin = p_str[1:] + m_str
        else:
            pos = p_str
            margin = m_str
            
    else:
        margin = prefix

    return pos, margin.strip(), section_time

# --- SESSION SETUP ---
def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Connection": "keep-alive"
    })
    return session

def log_msg(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} - {msg}\n")

# --- PARSING LOGIC ---
def parse_sectional_day(session, date_str):
    dt = datetime.strptime(date_str, "%Y/%m/%d")
    url_date = dt.strftime("%d/%m/%Y")
    
    base_url = f"https://racing.hkjc.com/racing/information/English/Racing/DisplaySectionalTime.aspx?RaceDate={url_date}&RaceNo=1"
    
    try:
        resp = session.get(base_url, timeout=15)
        if "No information found" in resp.text: return []
    except:
        return []

    day_results = []
    
    for race_no in range(1, 15):
        race_url = f"https://racing.hkjc.com/racing/information/English/Racing/DisplaySectionalTime.aspx?RaceDate={url_date}&RaceNo={race_no}"
        
        try:
            resp = session.get(race_url, timeout=10)
            if resp.status_code != 200: break
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            if "No information found" in soup.get_text(): continue

            # Robust Table Finder
            dfs = pd.read_html(StringIO(str(soup)))
            target_df = None
            for df in dfs:
                headers = [str(c).lower() for c in df.columns]
                if any('horse' in h for h in headers) and any('sec' in h for h in headers):
                    target_df = df
                    break
            
            if target_df is None: continue

            # Column Mapping
            if isinstance(target_df.columns, pd.MultiIndex):
                target_df.columns = [' '.join(col).strip() for col in target_df.columns.values]
            target_df.columns = [str(c).strip() for c in target_df.columns]
            
            col_map = {}
            for col in target_df.columns:
                c_low = col.lower()
                if 'no.' in c_low: col_map[col] = 'HorseNo'
                elif 'horse' in c_low: col_map[col] = 'HorseName'
                
                # STRICT FINISH TIME MATCHING
                # Exclusion Logic: If "Place", "Plc", "Rank" is in the header, ignore it.
                elif ('finish' in c_low or 'time' in c_low) and 'sec' not in c_low: 
                    if 'place' in c_low or 'plc' in c_low or 'rank' in c_low or 'order' in c_low:
                        continue
                    col_map[col] = 'FinishTime'
                
                elif 'sec' in c_low:
                    num = re.search(r'(\d+)', c_low)
                    if num: col_map[col] = f"Section_{num.group(1)}"

            target_df = target_df.rename(columns=col_map)
            
            # --- CRITICAL FIX: KEEP LAST DUPLICATE ---
            # Place (Rank) is usually on the Left. Time is on the Right.
            # If both accidentally map to 'FinishTime', keeping the LAST one usually gives the Time.
            target_df = target_df.loc[:, ~target_df.columns.duplicated(keep='last')]
            
            if 'HorseName' not in target_df.columns: continue

            # Row Processing
            for _, row in target_df.iterrows():
                
                h_raw = str(row.get('HorseName', '')).strip()
                if not h_raw or 'nan' in h_raw.lower(): continue
                
                brand_part = ""
                name_part = h_raw
                if '(' in h_raw:
                    parts = h_raw.split('(')
                    name_part = parts[0].strip()
                    brand_part = parts[1].replace(')', '').strip()
                
                # CLEAN FINISH TIME
                ft_val = row.get('FinishTime')
                if isinstance(ft_val, pd.Series): ft_val = ft_val.iloc[-1] # Safety: take last if still Series
                if pd.isna(ft_val): ft_val = None
                else: ft_val = str(ft_val).strip()

                row_data = {
                    "Date": date_str,
                    "RaceNo": race_no,
                    "HorseNo": row.get('HorseNo'),
                    "HorseName": name_part,
                    "BrandNo": brand_part,
                    "FinishTime": ft_val
                }

                # PROCESS SECTIONS
                for i in range(1, 7):
                    sec_key = f"Section_{i}"
                    raw_val = row.get(sec_key)
                    p, m, t = split_sectional_data(raw_val)
                    row_data[f"Sec{i}_Pos"] = p
                    row_data[f"Sec{i}_LBW"] = m
                    row_data[f"Sec{i}_Time"] = t

                day_results.append(row_data)

        except Exception as e:
            log_msg(f"Error {url_date} R{race_no}: {e}")
            continue

    return day_results

# --- WORKER ---
def worker_task(date_obj):
    session = create_session()
    date_str = date_obj.strftime("%Y/%m/%d")
    
    if (date_obj.month == 7 and date_obj.day > 16) or (date_obj.month == 8):
        session.close()
        return None

    try:
        time.sleep(random.uniform(0.5, 1.2))
        data = parse_sectional_day(session, date_str)
    except:
        data = []
    finally:
        session.close()
    return data

# --- MAIN ---
def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    
    headers = ["Date", "RaceNo", "HorseNo", "HorseName", "BrandNo", "FinishTime"]
    for i in range(1, 7):
        headers.extend([f"Sec{i}_Pos", f"Sec{i}_LBW", f"Sec{i}_Time"])
    
    if os.path.exists(DATA_FILE): os.remove(DATA_FILE)
    pd.DataFrame(columns=headers).to_csv(DATA_FILE, index=False)
    
    start = datetime.strptime(START_DATE, "%Y/%m/%d")
    end = datetime.strptime(END_DATE, "%Y/%m/%d")
    date_list = []
    curr = start
    while curr <= end:
        date_list.append(curr)
        curr += timedelta(days=1)
    
    print(f"--- HKJC SECTIONAL SCRAPER v13 (FIXED TIME COL) ---")
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
            
            if len(batch_data) >= 500:
                df = pd.DataFrame(batch_data)
                for h in headers:
                    if h not in df.columns: df[h] = None
                df = df[headers]
                df.to_csv(DATA_FILE, mode='a', header=False, index=False)
                total_saved += len(batch_data)
                batch_data = []
                gc.collect()

    if batch_data:
        df = pd.DataFrame(batch_data)
        for h in headers:
            if h not in df.columns: df[h] = None
        df = df[headers]
        df.to_csv(DATA_FILE, mode='a', header=False, index=False)
    
    print(f"\nDone. Total rows saved: {total_saved + len(batch_data)}")

if __name__ == "__main__":
    main()