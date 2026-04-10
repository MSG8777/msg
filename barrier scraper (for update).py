import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
import os
import concurrent.futures
from datetime import datetime, timedelta
import logging

# --- CONFIGURATION ---
DEFAULT_START_DATE = "2010/08/24" # Used if no existing file is found
BASE_URL = "https://racing.hkjc.com/racing/information/English/Horse/BTResult.aspx"

# SETTING THE OUTPUT PATH
OUTPUT_DIR = r"C:\HKJC_gemini_3_horse_racing_ML"
OUTPUT_FILENAME = "hkjc_barrier_trials_master_v7.csv"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

MAX_WORKERS = 12
SAVE_INTERVAL_DAYS = 10 

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

class HKJCProScraper:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://racing.hkjc.com/"
        }
        if not os.path.exists(OUTPUT_DIR):
            try:
                os.makedirs(OUTPUT_DIR)
                logger.info(f"Created directory: {OUTPUT_DIR}")
            except Exception as e:
                logger.error(f"Error creating directory: {e}")

    def get_latest_date_from_csv(self):
        """Checks the CSV and returns the latest date plus one day."""
        if not os.path.exists(OUTPUT_FILE):
            logger.info(f"File not found. Starting from default: {DEFAULT_START_DATE}")
            return datetime.strptime(DEFAULT_START_DATE, "%Y/%m/%d")
        
        try:
            # Only read the Date column for efficiency
            df = pd.read_csv(OUTPUT_FILE, usecols=['Date'])
            if df.empty:
                return datetime.strptime(DEFAULT_START_DATE, "%Y/%m/%d")
            
            # Convert to datetime and find max
            latest_date = pd.to_datetime(df['Date']).max()
            start_from = latest_date + timedelta(days=1)
            logger.info(f"Last record found: {latest_date.date()}. Resuming from: {start_from.date()}")
            return start_from
        except Exception as e:
            logger.error(f"Error reading CSV: {e}. Starting from default.")
            return datetime.strptime(DEFAULT_START_DATE, "%Y/%m/%d")

    def parse_header_text(self, text):
        """Parses 'Batch 1 - CONGHUA TURF - 1200m'"""
        info = {"Batch": None, "Venue": None, "Surface": None, "Distance": None}
        try:
            text = re.sub(r'\s+', ' ', text).strip()
            batch = re.search(r'Batch\s+(\d+)', text, re.IGNORECASE)
            if batch: info["Batch"] = batch.group(1)
            dist = re.search(r'(\d+)m', text, re.IGNORECASE)
            if dist: info["Distance"] = dist.group(1)

            if "-" in text:
                mid = text.split('-')[1].strip().upper()
                if "SHA TIN" in mid: info["Venue"] = "ST"
                elif "HAPPY VALLEY" in mid: info["Venue"] = "HV"
                elif "CONGHUA" in mid: info["Venue"] = "CH"
                
                if "TURF" in mid: info["Surface"] = "Turf"
                elif "ALL WEATHER" in mid or "AWT" in mid: info["Surface"] = "AWT"
        except: pass
        return info

    def parse_conditions_dictionary_mode(self, text):
        """Extracts Going, Time, Sectional using dictionary matching."""
        data = {"Going": None, "BatchTime": None, "LeaderSectional": None}
        text_upper = re.sub(r'\s+', ' ', text).strip().upper()
        
        time_match = re.search(r'TIME\s*[:\.]?\s*([\d\.]+)', text_upper)
        if time_match: data["BatchTime"] = time_match.group(1)
            
        sect_match = re.search(r'SECTIONAL TIME\s*[:\.]?\s*([\d\.\s]+)', text_upper)
        if sect_match: data["LeaderSectional"] = sect_match.group(1)

        VALID_GOINGS = [
            "GOOD TO FIRM", "GOOD TO YIELDING", "YIELDING TO SOFT", 
            "WET FAST", "WET SLOW", 
            "GOOD", "FIRM", "YIELDING", "SOFT", "HEAVY", "FAST", "SLOW", "NORMAL"
        ]
        
        for g in VALID_GOINGS:
            pattern = r'(^|[\s\.\-])' + re.escape(g) + r'($|[\s\.\-])'
            if re.search(pattern, text_upper):
                data["Going"] = g
                break
        return data

    def fetch_and_parse_date(self, date_str):
        """Scrapes and parses a single date's barrier trial data."""
        url = f"{BASE_URL}?Date={date_str}"
        records = []
        try:
            time.sleep(random.uniform(0.1, 0.4))
            resp = requests.get(url, headers=self.headers, timeout=20)
            if resp.status_code != 200 or "No information" in resp.text:
                return []

            soup = BeautifulSoup(resp.content, 'html.parser')
            tables = soup.find_all('table', class_=['bigborder', 'table_text_l'])

            for table in tables:
                header_info, condition_info = {}, {"Going": None, "BatchTime": None, "LeaderSectional": None}
                curr = table
                for _ in range(5):
                    curr = curr.find_previous_sibling()
                    if curr:
                        txt = curr.get_text(separator=' ', strip=True)
                        if "Batch" in txt and not header_info:
                            header_info = self.parse_header_text(txt)
                        found = self.parse_conditions_dictionary_mode(txt)
                        if found["Going"]: condition_info["Going"] = found["Going"]
                        if found["BatchTime"]: condition_info["BatchTime"] = found["BatchTime"]
                        if found["LeaderSectional"]: condition_info["LeaderSectional"] = found["LeaderSectional"]
                    else: break
                
                if not header_info.get("Batch"): continue

                rows = table.find_all('tr')
                for row in rows[1:]:
                    cols = row.find_all('td')
                    if len(cols) < 5: continue
                    def t(i): return cols[i].get_text(strip=True) if i < len(cols) else ""
                    raw_h = t(0)
                    brand_match = re.search(r'\(([A-Z0-9]+)\)', raw_h)
                    brand_no = brand_match.group(1) if brand_match else ""
                    horse_name = re.sub(r'\([A-Z0-9]+\)', '', raw_h).strip()

                    if not horse_name: continue
                    records.append({
                        "Date": date_str, "Batch": header_info.get("Batch"), "Venue": header_info.get("Venue"),
                        "Surface": header_info.get("Surface"), "Distance": header_info.get("Distance"),
                        "Going": condition_info.get("Going"), "BatchRawTime": condition_info.get("BatchTime"),
                        "LeaderSectional": condition_info.get("LeaderSectional"), "Horse": horse_name,
                        "BrandNo": brand_no, "Jockey": t(1), "Trainer": t(2), "Draw": t(3), "Gear": t(4),
                        "LBW": t(5), "RunningPosition": t(6), "FinishTime": t(7), "Result": t(8)
                    })
        except Exception as e:
            logger.error(f"Error on {date_str}: {e}")
        return records

    def save_chunk(self, data_chunk):
        """Appends a batch of results to the CSV file."""
        if not data_chunk: return
        df = pd.DataFrame(data_chunk)
        header_needed = not os.path.exists(OUTPUT_FILE)
        try:
            df.to_csv(OUTPUT_FILE, mode='a', header=header_needed, index=False, encoding='utf-8-sig')
            logger.info(f"Saved {len(data_chunk)} records.")
        except Exception as e:
            logger.error(f"Save failed: {e}")

    def run(self):
        """Main execution loop with incremental date logic."""
        # Determine the start and end dates
        s_date = self.get_latest_date_from_csv()
        e_date = datetime.today()
        
        all_dates = [(s_date + timedelta(days=i)).strftime("%Y/%m/%d") for i in range((e_date - s_date).days + 1)]
        
        if not all_dates:
            logger.info("Database is already up to date.")
            return

        logger.info(f"Starting Scrape from {s_date.date()} to {e_date.date()}")
        logger.info(f"Total Days to check: {len(all_dates)} | Workers: {MAX_WORKERS}")

        for i in range(0, len(all_dates), SAVE_INTERVAL_DAYS):
            chunk_dates = all_dates[i : i + SAVE_INTERVAL_DAYS]
            chunk_results = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_date = {executor.submit(self.fetch_and_parse_date, date): date for date in chunk_dates}
                for future in concurrent.futures.as_completed(future_to_date):
                    data = future.result()
                    if data:
                        chunk_results.extend(data)
                        logger.info(f"[{future_to_date[future]}] Found {len(data)} records.")

            if chunk_results:
                self.save_chunk(chunk_results)
            time.sleep(1)

        logger.info("Scraping Completed.")

if __name__ == "__main__":
    scraper = HKJCProScraper()
    scraper.run()
