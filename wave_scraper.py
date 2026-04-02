import os
import requests
import gspread
import json
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz
import traceback

# --- CONFIGURATION ---
# Target Google Sheet and Tab
SPREADSHEET_ID = '1a0NUGV_PngH8sO2ZoqoiqGyAEKwgCcvK04B2Gpu4g7Q'
SHEET_NAME = 'Wavetable'

# Timezone set to Melbourne
MELB_TZ = pytz.timezone('Australia/Melbourne')

# PERMANENTLY LOCKED PORT PHILLIP BAY NODES (Verified Apr 2026)
NODES = [
    {"name": "Mt Eliza", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11001?type=waves&simplified=1"},
    {"name": "Sandringham", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11011?type=waves&simplified=1"},
    {"name": "Central Bay", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11007?type=waves&simplified=1"}
]

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def fetch_data():
    """Extracts data and ensures numerical fields are properly typed."""
    now_melb = datetime.now(MELB_TZ)
    ext_date = now_melb.strftime("%d/%m/%Y")
    ext_time = now_melb.strftime("%H:%M")
    ext_timestamp = now_melb.strftime("%d/%m/%Y %H:%M")
    
    rows_to_append = []
    for node in NODES:
        obs_date, obs_time, obs_timestamp = "N/A", "N/A", "N/A"
        sig_wave, peak_period, peak_direction, wind_spd, wind_dir = "", "", "", "", ""
        node_display_name = node["name"]

        try:
            response = requests.get(node["url"], headers=HEADERS, timeout=15)
            if response.status_code == 200:
                payload = response.json().get("data", [])
                if payload:
                    latest = payload[0]
                    dt_melb = datetime.fromtimestamp(int(latest["time"]), pytz.utc).astimezone(MELB_TZ)
                    
                    obs_date = dt_melb.strftime("%d/%m/%Y")
                    obs_time = dt_melb.strftime("%H:%M")
                    obs_timestamp = dt_melb.strftime("%d/%m/%Y %H:%M")
                    
                    # Convert to float for Pivot Table compatibility
                    try:
                        sig_wave = float(latest.get("hsig", 0))
                        peak_period = float(latest.get("tp", 0))
                        peak_direction = float(latest.get("tpdeg", 0))
                        wind_spd = float(latest.get("windspeed", 0))
                        wind_dir = float(latest.get("winddirect", 0))
                    except (ValueError, TypeError):
                        sig_wave = latest.get("hsig", "")
            else:
                node_display_name += f" (Status {response.status_code})"
        except Exception:
            node_display_name += " (Fetch Error)"

        rows_to_append.append([
            obs_date, obs_time, obs_timestamp, node_display_name,
            sig_wave, peak_period, peak_direction, wind_spd,
            "", wind_dir, ext_date, ext_time, ext_timestamp
        ])
    return rows_to_append

def update_sheet(data):
    """Inserts new data at the top of the sheet (Row 2) and shifts old data down."""
    creds_raw = os.environ.get('GOOGLE_CREDS')
    if not creds_raw or not data:
        print("Missing Credentials or Data.")
        return

    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(json.loads(creds_raw), scopes=scope)
        client = gspread.authorize(creds)
        ss = client.open_by_key(SPREADSHEET_ID)
        sheet = ss.worksheet(SHEET_NAME)
        
        # 1. Insert blank rows at the top (starting at Row 2, below headers)
        num_new_rows = len(data)
        sheet.insert_rows(data, row=2, value_input_option='USER_ENTERED')
        
        print(f"SUCCESS: {num_new_rows} rows inserted at the TOP of {SHEET_NAME}")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    extracted_rows = fetch_data()
    update_sheet(extracted_rows)
