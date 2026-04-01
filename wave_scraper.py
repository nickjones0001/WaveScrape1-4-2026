import os
import requests
import gspread
import json
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- CONFIGURATION ---
# IMPORTANT: Ensure this matches your Sheet ID and Tab Name exactly
SPREADSHEET_ID = '1a0NUGV_PngH8sO2ZoqoiqGyAEKwgCcvK04B2Gpu4g7Q'
SHEET_NAME = 'Wavetable'

NODES = [
    {"name": "Mt Eliza", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11002?type=waves&simplified=1"},
    {"name": "Sandringham", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11003?type=waves&simplified=1"},
    {"name": "Central Bay", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11004?type=waves&simplified=1"}
]

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def fetch_data():
    now = datetime.now()
    ext_date = now.strftime("%d/%m/%Y")
    ext_time = now.strftime("%H:%M")
    ext_timestamp = now.strftime("%d/%m/%Y %H:%M")
    
    rows_to_append = []
    for node in NODES:
        obs_date, obs_time, obs_timestamp = "N/A", "N/A", "N/A"
        sig_wave, peak_per, peak_dir, wind_spd, wind_dir = "N/A", "N/A", "N/A", "N/A", "N/A"
        node_display_name = node["name"]

        try:
            response = requests.get(node["url"], headers=HEADERS, timeout=15)
            if response.status_code == 200:
                json_data = response.json()
                if json_data.get("data"):
                    latest = json_data["data"][0]
                    dt_object = datetime.fromtimestamp(latest.get("time", 0))
                    obs_date = dt_object.strftime("%d/%m/%Y")
                    obs_time = dt_object.strftime("%H:%M")
                    obs_timestamp = dt_object.strftime("%d/%m/%Y %H:%M")
                    sig_wave = latest.get("hsig", "N/A")
                    peak_per = latest.get("tp", "N/A")
                    peak_dir = latest.get("tpdeg", "N/A")
                    wind_spd = latest.get("windspeed", "N/A")
                    wind_dir = latest.get("winddirect", "N/A")
            else:
                node_display_name += f" (Status {response.status_code})"
        except Exception as e:
            node_display_name += " (Fetch Error)"

        rows_to_append.append([
            obs_date, obs_time, obs_timestamp, node_display_name,
            sig_wave, peak_per, peak_dir, wind_spd, "N/A", wind_dir,
            ext_date, ext_time, ext_timestamp
        ])
    return rows_to_append

def update_sheet(data):
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # Debug: Check if the Secret is even visible to the script
    creds_raw = os.environ.get('GOOGLE_CREDS')
    if not creds_raw:
        print("ERROR: GOOGLE_CREDS environment variable is empty. Check GitHub Secrets.")
        return

    try:
        creds_json = json.loads(creds_raw)
        creds = Credentials.from_service_account_info(creds_json, scopes=scope)
        client = gspread.authorize(creds)
        
        # Attempt to open the sheet
        ss = client.open_by_key(SPREADSHEET_ID)
        sheet = ss.worksheet(SHEET_NAME)
        
        sheet.append_rows(data)
        print(f"SUCCESS: {len(data)} rows added to {SHEET_NAME}")
    except gspread.exceptions.WorksheetNotFound:
        print(f"ERROR: Worksheet '{SHEET_NAME}' not found in the spreadsheet.")
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")

if __name__ == "__main__":
    extracted_rows = fetch_data()
    update_sheet(extracted_rows)
