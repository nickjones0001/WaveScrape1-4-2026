import os
import requests
import gspread
import json
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- CONFIGURATION ---
# Replace with your actual Spreadsheet ID from the URL
SPREADSHEET_ID = 'Y1a0NUGV_PngH8sO2ZoqoiqGyAEKwgCcvK04B2Gpu4g7Q'
SHEET_NAME = 'Wavetable'

# AusWaves Nodes mapping
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
    """Extracts data and formats for a 13-column granular structure."""
    now = datetime.now()
    ext_date = now.strftime("%d/%m/%Y")
    ext_time = now.strftime("%H:%M")
    ext_timestamp = now.strftime("%d/%m/%Y %H:%M")
    
    rows_to_append = []

    for node in NODES:
        obs_date, obs_time, obs_timestamp = "N/A", "N/A", "N/A"
        sig_wave, peak_per, peak_dir = "N/A", "N/A", "N/A"
        wind_spd, wind_dir = "N/A", "N/A"
        node_display_name = node["name"]

        try:
            response = requests.get(node["url"], headers=HEADERS, timeout=15)
            
            if response.status_code == 200:
                json_data = response.json()
                if json_data.get("data") and len(json_data["data"]) > 0:
                    latest = json_data["data"][0]

                    # Extraction of native buoy time
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
                    node_display_name += " (No Payload)"
            else:
                node_display_name += f" (HTTP {response.status_code})"
                
        except Exception as e:
            node_display_name += " (Error)"

        # 13-column row structure
        row = [
            obs_date,           # Col A
            obs_time,           # Col B
            obs_timestamp,      # Col C
            node_display_name,  # Col D
            sig_wave,           # Col E
            peak_per,           # Col F
            peak_dir,           # Col G
            wind_spd,           # Col H
            "N/A",              # Col I
            wind_dir,           # Col J
            ext_date,           # Col K
            ext_time,           # Col L
            ext_timestamp       # Col M
        ]
        rows_to_append.append(row)
    
    return rows_to_append

def update_sheet(data):
    """Writes the batch of data to Google Sheets."""
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    try:
        if os.environ.get('GOOGLE_CREDS'):
            creds_json = json.loads(os.environ.get('GOOGLE_CREDS'))
            creds = Credentials.from_service_account_info(creds_json, scopes=scope)
        else:
            creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
            
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
        
        sheet.append_rows(data)
        print(f"Update completed: {len(data)} rows added to {SHEET_NAME}")
    except Exception as e:
        print(f"Error during update: {e}")

if __name__ == "__main__":
    extracted_rows = fetch_data()
    update_sheet(extracted_rows)
