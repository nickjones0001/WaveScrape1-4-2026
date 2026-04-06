import gspread
from google.oauth2.service_account import Credentials
import requests
import datetime
import pytz
import os
import json

# Configuration
SHEET_NAME = "Wind+WaveScrapeLLM 28-3-2026"
DATA_TAB = "Wind+Dir"
TIMEZONE = pytz.timezone('Australia/Melbourne')

DIRECTION_ARROWS = {"N":"↑","NNE":"↗","NE":"↗","ENE":"→","E":"→","ESE":"↘","SE":"↘","SSE":"↓","S":"↓","SSW":"↙","SW":"↙","WSW":"←","W":"←","WNW":"↖","NW":"↖","NNW":"↑","CALM":"○"}

def get_wind_data():
    results = []
    now_melbourne = datetime.datetime.now(TIMEZONE)
    headers = {'User-Agent': 'Mozilla/5.0'}
    stations = {
        "Frankston Beach": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94870.json",
        "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94864.json",
        "South Channel Island": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94857.json"
    }
    for name, url in stations.items():
        try:
            res = requests.get(url, headers=headers).json()
            obs = res['observations']['data'][0]
            ts = obs['local_date_time_full']
            # Format as a full Date/Time string so Google recognizes it
            label = f"{ts[6:8]}/{ts[4:6]}/{ts[0:4]} {ts[8:10]}:{ts[10:12]}"
            results.append([
                f"{ts[6:8]}/{ts[4:6]}/{ts[0:4]}", f"{ts[8:10]}:{ts[10:12]}", name,
                float(obs.get('wind_spd_kt', 0)), DIRECTION_ARROWS.get(obs.get('wind_dir', '-'), "-"),
                obs.get('wind_dir', '-'), now_melbourne.strftime("%d/%m/%Y"),
                now_melbourne.strftime("%H:%M:%S"), label
            ])
        except: continue
    return results

def update_sheet():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json: return
    creds = Credentials.from_service_account_info(json.loads(creds_json), 
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    sh = client.open(SHEET_NAME)
    ws = sh.worksheet(DATA_TAB)

    new_data = get_wind_data()
    if new_data:
        # Insert at Row 2. Standard Chart (I:I and D:D) updates automatically.
        ws.insert_rows(new_data, row=2)
        print(f"Data added. Chart will grow into its manual stretch.")

if __name__ == "__main__":
    update_sheet()
