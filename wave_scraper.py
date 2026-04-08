# --- COPY START ---
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
DATA_SHEET_NAME = 'Wavetable'

MELB_TZ = pytz.timezone('Australia/Melbourne')

NODES = [
    {"name": "Mt Eliza", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11001?type=waves&simplified=1"},
    {"name": "Sandringham", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11011?type=waves&simplified=1"},
    {"name": "Central Bay", "url": "https://auswaves.org/wp-json/waves/v1/buoys/11007?type=waves&simplified=1"}
]

HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}

def fetch_data():
    now_melb = datetime.now(MELB_TZ)
    ext_date, ext_time = now_melb.strftime("%d/%m/%Y"), now_melb.strftime("%H:%M")
    rows = []
    for node in NODES:
        try:
            res = requests.get(node["url"], headers=HEADERS, timeout=15)
            if res.status_code == 200:
                p = res.json().get("data", [{}])[0]
                # Standardizing timestamp to Australia/Melbourne
                dt = datetime.fromtimestamp(int(p["time"]), pytz.utc).astimezone(MELB_TZ)
                
                # Converting numerical strings to floats for Data Integrity
                rows.append([
                    dt.strftime("%Y-%m-%d"), 
                    dt.strftime("%H:%M"), 
                    dt.strftime("%Y-%m-%d %H:%M"),
                    node["name"], 
                    float(p.get("hsig", 0)), 
                    float(p.get("tp", 0)),
                    float(p.get("tpdeg", 0)), 
                    float(p.get("windspeed", 0)), 
                    "",  # Placeholder for missing column
                    float(p.get("winddirect", 0)), 
                    ext_date, 
                    ext_time, 
                    f"{ext_date} {ext_time}"
                ])
        except Exception:
            pass
    return rows

def update_maritime_system(data):
    creds_raw = os.environ.get('GOOGLE_CREDS')
    if not creds_raw or not data:
        print("Error: Missing credentials or no data fetched.")
        return

    try:
        # 1. AUTHENTICATION
        creds_info = json.loads(creds_raw)
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        client = gspread.authorize(creds)
        
        # 2. OPEN SHEET
        ss = client.open_by_key(SPREADSHEET_ID)
        data_sheet = ss.worksheet(DATA_SHEET_NAME)
        
        # 3. PUSH DATA
        # insert_rows at row=2 ensures newest data is at the top.
        # value_input_option='USER_ENTERED' ensures decimals and dates are recognized correctly.
        data_sheet.insert_rows(data, row=2, value_input_option='USER_ENTERED')
        
        print(f"SUCCESS: {len(data)} rows pushed to {DATA_SHEET_NAME} at Row 2.")

    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    current_data = fetch_data()
    update_maritime_system(current_data)
# --- COPY END ---
