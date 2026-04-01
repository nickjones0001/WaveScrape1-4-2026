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

# VERIFIED PORT PHILLIP BAY PERMANENT LOCK
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
    """Extracts data from verified buoy IDs and formats for Google Sheets."""
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
                    # Convert buoy time to Melbourne Local
                    dt_melb = datetime.fromtimestamp(int(latest["time"]), pytz.utc).astimezone(MELB_TZ)
                    
                    obs_date = dt_melb.strftime("%d/%m/%Y")
                    obs_time = dt_melb.strftime("%H:%M")
                    obs_timestamp = dt_melb.strftime("%d/%m/%Y %H:%M")
                    
                    sig_wave = latest.get("hsig", "")
                    peak_period = latest.get("tp", "")
                    peak_direction = latest.get("tpdeg", "")
                    wind_spd = latest.get("windspeed", "")
                    wind_dir = latest.get("winddirect", "")
            else:
                node_display_name += f" (HTTP {response.status_code})"
        except Exception as e:
            node_display_name += " (Script Error)"

        # Append row in 13-column structure (A through M)
        rows_to_append.append([
            obs_date,           # A
            obs_time,           # B
            obs_timestamp,      # C
            node_display_name,  # D
            sig_wave,           # E
            peak_period,        # F
            peak_direction,     # G
            wind_spd,           # H
            "",                 # I (Gusts placeholder)
            wind_dir,           # J
            ext_date,           # K
            ext_time,           # L
            ext_timestamp       # M
        ])
    return rows_to_append

def update_sheet(data):
    """Authenticates and writes the batch to the first empty row."""
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
        
        # Calculate start row based on Column D (Node Name)
        next_row = len(sheet.col_values(4)) + 1
        if next_row < 2:
            next_row = 2
            
        end_row = next_row + len(data) - 1
        range_to_update = f"A{next_row}:M{end_row}"
        
        sheet.update(range_name=range_to_update, values=data)
        print(f"SUCCESS: Logged {len(data)} rows to {SHEET_NAME} starting at Row {next_row}")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    extracted_rows = fetch_data()
    update_sheet(extracted_rows)
