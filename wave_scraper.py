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

# Explicitly set Melbourne Timezone
MELB_TZ = pytz.timezone('Australia/Melbourne')

# VERIFIED PORT PHILLIP BAY NODES:
# 11001 = Mt Eliza
# 11003 = Sandringham
# 11005 = Central Bay
NODES = [
    {"name": "Mt Eliza", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11001?type=waves&simplified=1"},
    {"name": "Sandringham", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11003?type=waves&simplified=1"},
    {"name": "Central Bay", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11005?type=waves&simplified=1"}
]

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def fetch_data():
    print("Starting data fetch from AusWaves...")
    
    # Capture current Melbourne time for Extraction columns (K, L, M)
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
                json_data = response.json()
                if json_data.get("data") and len(json_data["data"]) > 0:
                    # Get the most recent observation (index 0)
                    latest = json_data["data"][0]
                    
                    # Convert buoy Unix time to Melbourne Time (Col A, B, C)
                    raw_time = latest.get("time")
                    if raw_time:
                        # Ensure we convert the integer timestamp correctly to Melb time
                        dt_utc = datetime.fromtimestamp(int(raw_time), pytz.utc)
                        dt_melb = dt_utc.astimezone(MELB_TZ)
                        
                        obs_date = dt_melb.strftime("%d/%m/%Y")
                        obs_time = dt_melb.strftime("%H:%M")
                        obs_timestamp = dt_melb.strftime("%d/%m/%Y %H:%M")
                    
                    sig_wave = latest.get("hsig", "")
                    peak_period = latest.get("tp", "")
                    peak_direction = latest.get("tpdeg", "")
                    wind_spd = latest.get("windspeed", "")
                    wind_dir = latest.get("winddirect", "")
                    
                    print(f"Fetched {node['name']}: {sig_wave}m @ {obs_time}")
                else:
                    node_display_name += " (No Data)"
            else:
                node_display_name += f" (HTTP {response.status_code})"
        except Exception as e:
            print(f"Error processing {node['name']}: {str(e)}")
            node_display_name += " (Script Error)"

        # Rows align to your 13-column headers (A through M)
        rows_to_append.append([
            obs_date, obs_time, obs_timestamp, node_display_name,
            sig_wave, peak_period, peak_direction, wind_spd, "", wind_dir,
            ext_date, ext_time, ext_timestamp
        ])
    return rows_to_append

def update_sheet(data):
    print("Connecting to Google Sheets...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_raw = os.environ.get('GOOGLE_CREDS')
    
    if not creds_raw:
        print("CRITICAL ERROR: GOOGLE_CREDS environment variable is missing!")
        return

    try:
        creds_json = json.loads(creds_raw)
        creds = Credentials.from_service_account_info(creds_json, scopes=scope)
        client = gspread.authorize(creds)
        ss = client.open_by_key(SPREADSHEET_ID)
        sheet = ss.worksheet(SHEET_NAME)
        
        # Calculate next empty row based on the Node Name column (Col D)
        col_d_values = sheet.col_values(4)
        next_row = len(col_d_values) + 1
        if next_row < 2:
            next_row = 2

        print(f"Targeting Row: {next_row}")
        
        end_row = next_row + len(data) - 1
        range_to_update = f"A{next_row}:M{end_row}"
        
        # Update the sheet starting from the first empty row
        sheet.update(range_name=range_to_update, values=data)
        print(f"SUCCESS: Logged to {SHEET_NAME} at Melbourne Time: {datetime.now(MELB_TZ).strftime('%H:%M:%S')}")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    extracted_rows = fetch_data()
    update_sheet(extracted_rows)
