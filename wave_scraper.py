import os
import requests
import gspread
import json
from google.oauth2.service_account import Credentials
from datetime import datetime
import traceback

# --- CONFIGURATION ---
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
    print("Starting data fetch from AusWaves...")
    now = datetime.now()
    ext_date = now.strftime("%d/%m/%Y")
    ext_time = now.strftime("%H:%M")
    ext_timestamp = now.strftime("%d/%m/%Y %H:%M")
    
    rows_to_append = []
    for node in NODES:
        obs_date, obs_time, obs_timestamp = "N/A", "N/A", "N/A"
        sig_wave, peak_per, peak_dir, wind_spd, wind_dir = "", "", "", "", ""
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
                    sig_wave = latest.get("hsig", "")
                    peak_per = latest.get("tp", "")
                    peak_dir = latest.get("tpdeg", "")
                    wind_spd = latest.get("windspeed", "")
                    wind_dir = latest.get("winddirect", "")
            else:
                print(f"Warning: {node['name']} returned status {response.status_code}")
        except Exception as e:
            print(f"Error fetching {node['name']}: {e}")

        rows_to_append.append([
            obs_date, obs_time, obs_timestamp, node_display_name,
            sig_wave, peak_per, peak_dir, wind_spd, "", wind_dir,
            ext_date, ext_time, ext_timestamp
        ])
    return rows_to_append

def update_sheet(data):
    print("Attempting to connect to Google Sheets...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_raw = os.environ.get('GOOGLE_CREDS')
    
    if not creds_raw:
        print("CRITICAL ERROR: GOOGLE_CREDS environment variable is missing!")
        return

    try:
        print("Parsing credentials...")
        creds_json = json.loads(creds_raw)
        creds = Credentials.from_service_account_info(creds_json, scopes=scope)
        client = gspread.authorize(creds)
        
        print(f"Opening Spreadsheet ID: {SPREADSHEET_ID}")
        ss = client.open_by_key(SPREADSHEET_ID)
        
        print(f"Accessing Worksheet: {SHEET_NAME}")
        sheet = ss.worksheet(SHEET_NAME)
        
        print("Appending data...")
        sheet.append_rows(data)
        print(f"SUCCESS: {len(data)} rows added.")
        
    except json.JSONDecodeError:
        print("CRITICAL ERROR: The GOOGLE_CREDS secret is not valid JSON. Check for stray characters.")
    except gspread.exceptions.SpreadsheetNotFound:
        print("CRITICAL ERROR: Spreadsheet ID not found. Check the ID in the script.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"CRITICAL ERROR: Tab named '{SHEET_NAME}' not found.")
    except gspread.exceptions.APIError as e:
        print(f"CRITICAL ERROR: Google API returned an error. Is the sheet shared with the service account email? Detail: {e}")
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    extracted_rows = fetch_data()
    update_sheet(extracted_rows)
