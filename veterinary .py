import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import time
import random
import re
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter, Retry

# --- CONFIGURATION ---
OUTPUT_DIR = r"C:\HKJC_gemini_3_horse_racing_ML"
FINAL_DB_FILE = os.path.join(OUTPUT_DIR, "hkjc_vet_db_REVERSE.csv")

# SCAN BACKWARDS (To prove it works immediately)
START_DATE = "2026/01/02" 
END_DATE = "1979/09/01" 
MAX_WORKERS = 20

# --- VET PATTERNS (For when data is in text paragraphs) ---
VET_PATTERNS = {
    'Bleeding': r'\b(bled from both nostrils|blood in trachea|substantial amount of blood|epistaxis)\b',
    'Heart': r'\b(heart irregularity|irregular heart rhythm|atrial fibrillation)\b',
    'Lame': r'\b(lame|claudication|restricted action|swollen|fettered)\b',
    'Respiratory': r'\b(mucus|roarer|epiglottic|respiratory|airway)\b',
    'Injury': r'\b(injury|wound|abrasion|cut|laceration|fracture|sole bruise)\b'
}

def create_session():
    session = requests.Session()
    retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
    })
    return session

def parse_race(session, date_str, race_no):
    # Standard URL for Results (Most reliable source for Incidents)
    url_date = date_str.replace("-", "/")
    url = f"https://racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx?RaceDate={url_date}&RaceNo={race_no}"
    
    try:
        resp = session.get(url, timeout=8)
        if "No information found" in resp.text:
            return []
            
        soup = BeautifulSoup(resp.content, 'html.parser')
        records = []
        
        # --- STRATEGY A: THE INCIDENT TABLE (Common in 2010-2025) ---
        # We look for ANY table that has "Horse" and "Incident/Report/Remarks" columns
        tables = soup.find_all('table')
        
        table_found = False
        for table in tables:
            # Get headers (handle th and td)
            headers = [th.get_text(strip=True).lower() for th in table.find_all('tr')[0].find_all(['th', 'td'])]
            
            # Map columns
            horse_idx = -1
            incident_idx = -1
            
            for i, h in enumerate(headers):
                if 'horse' in h and 'no' not in h: horse_idx = i
                if any(x in h for x in ['incident', 'report', 'remarks', 'detail']): incident_idx = i
            
            # If we found a valid table structure
            if horse_idx != -1 and incident_idx != -1:
                table_found = True
                rows = table.find_all('tr')[1:] # Skip header
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) > max(horse_idx, incident_idx):
                        horse = cols[horse_idx].get_text(strip=True)
                        incident = cols[incident_idx].get_text(strip=True)
                        
                        # Clean Horse Name (Remove BrandNo if present for CSV consistency)
                        # e.g. "GOLDEN SIXTY (C238)" -> "GOLDEN SIXTY"
                        horse_clean = re.split(r'\s*\(', horse)[0].strip()
                        
                        if len(incident) > 3 and "No report" not in incident:
                            records.append({
                                "Date": date_str,
                                "RaceNo": race_no,
                                "HorseName": horse_clean,
                                "Details": incident,
                                "Source": "Table"
                            })
                break # Stop searching tables if we found the main one
        
        if table_found:
            return records

        # --- STRATEGY B: PARAGRAPH MINING (Common in 2002-2009) ---
        # If no clean table exists, we scan the whole page text
        page_text = soup.get_text(" ", strip=True)
        sentences = re.split(r'(?<!\d)\.(?!\d)', page_text)
        
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) < 15: continue
            
            # Check for Vet Keywords
            for cat, pat in VET_PATTERNS.items():
                if re.search(pat, sentence, re.IGNORECASE):
                    # Found a vet issue, look for a Horse Name (Capitalized)
                    caps = re.findall(r'\b[A-Z]{2,}(?:\s+[A-Z]{2,})*\b', sentence)
                    ignored = ['THE', 'HKJC', 'VET', 'DR', 'MR', 'MRS', 'TRACK', 'COURSE', 'RACE', 
                               'AFTER', 'BEFORE', 'PASSING', 'METRES', 'FINISH', 'START', 'TURF', 
                               'PAGE', 'DATE', 'GOING', 'INCIDENT', 'REPORT', 'DIVIDENDS', 'SUMMARY']
                    valid_horses = [h for h in caps if h not in ignored and len(h) > 2]
                    
                    if valid_horses:
                        horse = valid_horses[0]
                        if "SUBSTANTIAL" in horse: continue
                        
                        records.append({
                            "Date": date_str,
                            "RaceNo": race_no,
                            "HorseName": horse,
                            "Details": sentence[:200],
                            "Source": "TextMining"
                        })
                        break # Next sentence
        return records

    except Exception:
        return []

def worker(date_obj):
    session = create_session()
    date_str = date_obj.strftime("%Y/%m/%d")
    
    # Summer Break
    if (date_obj.month == 7 and date_obj.day > 16) or (date_obj.month == 8):
        session.close()
        return None

    data = []
    try:
        # Check races 1 to 12
        for r in range(1, 13):
            r_data = parse_race(session, date_str, r)
            if r_data:
                data.extend(r_data)
            elif r == 1: 
                # If Race 1 is empty, the day is likely empty
                break
    except:
        pass
    finally:
        session.close()
    return data

def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    
    # Init CSV
    if os.path.exists(FINAL_DB_FILE): os.remove(FINAL_DB_FILE)
    pd.DataFrame(columns=["Date", "RaceNo", "HorseName", "Details", "Source"]).to_csv(FINAL_DB_FILE, index=False)
    
    # GENERATE DATES REVERSE
    start = datetime.strptime(START_DATE, "%Y/%m/%d")
    end = datetime.strptime(END_DATE, "%Y/%m/%d")
    delta = (start - end).days
    
    # Create list going from 2025 DOWN to 1997
    date_list = [start - timedelta(days=x) for x in range(delta + 1)]
    
    print(f"--- HKJC VET SCRAPER (REVERSE ORDER) ---")
    print(f"Scanning from {START_DATE} backwards to {END_DATE}")
    print(f"Target: {FINAL_DB_FILE}")
    
    batch = []
    total = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(worker, d): d for d in date_list}
        
        for i, future in enumerate(as_completed(future_map)):
            res = future.result()
            if res:
                batch.extend(res)
            
            if i % 20 == 0:
                sys.stdout.write(f"\rDays Processed: {i}/{len(date_list)} | Records Found: {total + len(batch)}")
                sys.stdout.flush()
                
            if len(batch) >= 50:
                pd.DataFrame(batch).to_csv(FINAL_DB_FILE, mode='a', header=False, index=False)
                total += len(batch)
                batch = []

    if batch:
        pd.DataFrame(batch).to_csv(FINAL_DB_FILE, mode='a', header=False, index=False)
        total += len(batch)

    print(f"\nDone. Total Records: {total}")

if __name__ == "__main__":
          main()

