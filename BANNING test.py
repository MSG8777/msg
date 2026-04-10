import requests
import random
import time

def check_hkjc_access():
    url = "https://bet.hkjc.com/football/index.aspx"  # Main betting page
    
    # Professional headers to look like a real browser, not a bot
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com/"
    }

    try:
        print(f"Testing connection to {url}...")
        start_time = time.time()
        
        # Timeout set to 10s to detect firewall drops
        response = requests.get(url, headers=headers, timeout=10)
        
        elapsed = time.time() - start_time
        status = response.status_code

        print(f"Status Code: {status}")
        print(f"Response Time: {elapsed:.2f}s")

        if status == 200:
            if "Enable JavaScript" in response.text or "Challenge" in response.text:
                print("⚠️  Status 200, but caught in a JavaScript Challenge.")
                print("Diagnosis: NOT BANNED, but your current scraper cannot read the data. You need Selenium or Playwright.")
            else:
                print("✅  Success: Your IP is clean and the site is accessible.")
        elif status == 403:
            print("⛔  CRITICAL: 403 Forbidden. Your IP is likely flagged/banned.")
        elif status == 429:
            print("⚠️  WARNING: 429 Too Many Requests. You are being rate-limited. Stop immediately for 24 hours.")
        else:
            print(f"⚠️  Unknown Status: {status}. Check your internet or proxy settings.")

    except requests.exceptions.ConnectTimeout:
        print("⛔  TIMEOUT: The server is dropping your connection. High likelihood of IP Ban.")
    except Exception as e:
        print(f"❌  Error: {e}")

if __name__ == "__main__":
    check_hkjc_access()
