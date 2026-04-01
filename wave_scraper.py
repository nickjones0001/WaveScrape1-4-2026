import os
import requests
import gspread
import json
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz
import traceback

# --- CONFIGURATION ---
SPREADSHEET_ID = '1a0NUGV_PngH8sO2ZoqoiqGyAEKwgCcvK04B2Gpu4g7Q'
SHEET_NAME = 'Wavetable'
MELB_TZ = pytz.timezone('Australia/Melbourne')

# MT ELIZA AND CENTRAL BAY ARE NOW LOCKED
LOCKED_NODES = [
    {"name": "Mt Eliza", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11001?type=waves&simplified=1"},
    {"name": "Central Bay", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11007?type=waves&simplified=1"}
]

# NEW DIAGNOSTIC RANGE: Looking for Sandringham (0.33m / 3s / 224° SW)
DIAGNOSTIC_IDS = [11009, 11010, 11011, 11012, 11013, 11014, 11015]

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def run_diagnostics():
    print("--- STARTING DEVELOPER DIAGNOSTICS (PHASE 2) ---")
    print("Checking higher IDs to find the Sandringham match:")
    for buoy_id in DIAGNOSTIC_IDS:
        url = f"https://auswaves.org/wp-json/waves/v1/buoys/{buoy_id}?type=waves&simplified=1"
        try:
            res = requests.get(url, headers=HEADERS, timeout=10)
            if res.status_code == 200:
                data = res.json().get("data", [])
                if data:
                    obs = data[0]
                    print(f"ID {buoy_id} >> Ht: {obs.get('hsig')}m | Per: {obs.get('tp')}s | Dir: {obs.get('tpdeg')}°")
                else:
                    print(f"ID {buoy_id} >> No Payload")
        except:
            pass
    print("--- END OF DIAGNOSTICS ---")

def fetch_locked_data():
    now_melb = datetime.now(MELB_TZ)
    ext_date = now_melb.strftime("%d/%m/%Y")
    ext_time = now_melb.strftime("%H:%M")
    ext_timestamp = now_melb.strftime("%d/%m/%Y %H:%M")
    
    rows = []
    for node in LOCKED_NODES:
        try:
            response = requests.get(node["url"], headers=HEADERS, timeout=15)
            if response.status_code == 200:
                payload = response.json().get("data", [])
                if payload:
                    latest = payload[0]
                    dt_melb = datetime.fromtimestamp(int(latest["time"]), pytz.utc).astimezone(MELB_TZ)
                    
                    rows.append([
                        dt_melb.strftime("%d/%m/%Y"),
                        dt_melb.strftime("%H:%M"),
                        dt_melb.strftime("%d/%m/%Y %H:%M"),
                        node["name"],
                        latest.get("hsig", ""),
                        latest.get("tp", ""),
                        latest.get("tpdeg", ""),
                        latest.get("windspeed", ""),
                        "", # Gusts placeholder
                        latest.get("winddirect", ""),
                        ext_date,
                        ext_time,
                        ext_timestamp
                    ])
        except Exception as e:
            print(f"Error fetching {node['name']}: {e}")
    return rows

def update_sheet(data):
    creds_raw = os.environ.get('GOOGLE_CREDS')
    if not creds_raw or not data: return

    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(json.loads(creds_raw), scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
        
        # Calculate next row based on Column D (Node)
        next_row = len(sheet.col_values(4)) + 1
        if next_row < 2: next_row = 2
        
        range_label = f"A{next_row}:M{next_row + len(data) - 1}"
        sheet.update(range_name=range_label, values=data)
        print(f"SUCCESS: Logged {len(data)} nodes starting at Row {next_row}")
    except Exception as e:
        print(f"Sheets Error: {e}")

if __name__ == "__main__":
    run_diagnostics()
    rows = fetch_locked_data()
    update_sheet(rows)
