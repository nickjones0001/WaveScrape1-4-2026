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
PIVOT_SHEET_NAME = 'Wavetable-pivotInfinite'

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
                dt = datetime.fromtimestamp(int(p["time"]), pytz.utc).astimezone(MELB_TZ)
                rows.append([
                    dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M"), dt.strftime("%Y-%m-%d %H:%M"),
                    node["name"], float(p.get("hsig", 0)), float(p.get("tp", 0)),
                    float(p.get("tpdeg", 0)), float(p.get("windspeed", 0)), "", 
                    float(p.get("winddirect", 0)), ext_date, ext_time, f"{ext_date} {ext_time}"
                ])
        except: pass
    return rows

def update_maritime_system(data):
    creds_raw = os.environ.get('GOOGLE_CREDS')
    if not creds_raw or not data: return

    try:
        # 1. AUTH & TOKEN
        creds_info = json.loads(creds_raw)
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        
        import google.auth.transport.requests
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        access_token = creds.token
        
        client = gspread.authorize(creds)
        ss = client.open_by_key(SPREADSHEET_ID)
        data_sheet = ss.worksheet(DATA_SHEET_NAME)
        pivot_sheet = ss.worksheet(PIVOT_SHEET_NAME)
        
        # 2. PUSH DATA
        data_sheet.insert_rows(data, row=2, value_input_option='USER_ENTERED')
        print(f"SUCCESS: Data pushed to {DATA_SHEET_NAME}")

        # 3. GET IDS
        meta_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}"
        spreadsheet = requests.get(meta_url, headers={"Authorization": f"Bearer {access_token}"}).json()
        
        target_sheet_id, target_chart_id = None, None
        for s in spreadsheet.get('sheets', []):
            if s['properties']['title'] == PIVOT_SHEET_NAME:
                target_sheet_id = s['properties']['sheetId']
                if 'charts' in s: target_chart_id = s['charts'][0]['chartId']

        if target_chart_id:
            # 4. CALC SCALE
            pivot_rows = pivot_sheet.get_values("A:A")
            row_count = len([r for r in pivot_rows if r and r[0]])
            calc_width = max(1200, int((row_count * 68) + 250))

            # 5. THE WILDCARD REQUEST
            # Using fields: "*" bypasses the need to name "newPosition"
            batch_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}:batchUpdate"
            
            payload = {
                "requests": [{
                    "updateEmbeddedObjectPosition": {
                        "objectId": target_chart_id,
                        "newPosition": {
                            "overlayPosition": {
                                "anchorCell": {
                                    "sheetId": target_sheet_id, 
                                    "rowIndex": 1, 
                                    "columnIndex": 6
                                },
                                "widthPixels": calc_width,
                                "heightPixels": 585
                            }
                        },
                        "fields": "*" 
                    }
                }]
            }

            response = requests.post(
                batch_url,
                headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                data=json.dumps(payload)
            )

            if response.status_code == 200:
                print(f"SUCCESS: Chart container set to {calc_width}x585 via Wildcard.")
            else:
                print(f"API ERROR: {response.status_code} - {response.text}")

    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    update_maritime_system(fetch_data())
