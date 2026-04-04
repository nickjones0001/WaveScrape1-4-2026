import os
import requests
import gspread
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
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
    ext_date = now_melb.strftime("%d/%m/%Y")
    ext_time = now_melb.strftime("%H:%M")
    
    rows_to_append = []
    for node in NODES:
        try:
            response = requests.get(node["url"], headers=HEADERS, timeout=15)
            if response.status_code == 200:
                payload = response.json().get("data", [])[0]
                dt_melb = datetime.fromtimestamp(int(payload["time"]), pytz.utc).astimezone(MELB_TZ)
                
                rows_to_append.append([
                    dt_melb.strftime("%Y-%m-%d"), 
                    dt_melb.strftime("%H:%M"),    
                    dt_melb.strftime("%Y-%m-%d %H:%M"), 
                    node["name"],
                    float(payload.get("hsig", 0)), 
                    float(payload.get("tp", 0)),   
                    float(payload.get("tpdeg", 0)),
                    float(payload.get("windspeed", 0)),
                    "", 
                    float(payload.get("winddirect", 0)),
                    ext_date, ext_time, f"{ext_date} {ext_time}"
                ])
        except Exception as e:
            print(f"Fetch Error for {node['name']}: {e}")
            
    return rows_to_append

def update_maritime_system(data):
    creds_raw = os.environ.get('GOOGLE_CREDS')
    if not creds_raw or not data: return

    try:
        # 1. AUTHENTICATION
        creds_info = json.loads(creds_raw)
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        
        # Gspread for Data Insertion
        client = gspread.authorize(creds)
        ss = client.open_by_key(SPREADSHEET_ID)
        data_sheet = ss.worksheet(DATA_SHEET_NAME)
        pivot_sheet = ss.worksheet(PIVOT_SHEET_NAME)
        
        # 2. INSERT RAW DATA
        data_sheet.insert_rows(data, row=2, value_input_option='USER_ENTERED')
        print(f"Data pushed to {DATA_SHEET_NAME}")

        # 3. CHART RESIZING (Sheets API v4)
        service = build('sheets', 'v4', credentials=creds)
        
        # Get sheet metadata to find Chart ID and Sheet ID
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        
        target_sheet_id = None
        target_chart_id = None
        
        for s in spreadsheet['sheets']:
            if s['properties']['title'] == PIVOT_SHEET_NAME:
                target_sheet_id = s['properties']['sheetId']
                if 'charts' in s and len(s['charts']) > 0:
                    target_chart_id = s['charts'][0]['chartId']

        if target_chart_id:
            # Count rows in Pivot Table (Column A) for scaling
            pivot_rows = pivot_sheet.get_values("A:A")
            last_row_count = len([r for r in pivot_rows if r and r[0]])
            
            # DIMENSIONS: 68px per row (+50% width), 585px height (+30% height)
            calc_width = int((last_row_count * 68) + 250)
            if calc_width < 1200: calc_width = 1200
            calc_height = 585 

            # API Request: Only modifies the Position and Size (Preserves manual styles)
            requests_body = {
                "requests": [{
                    "updateEmbeddedObjectPosition": {
                        "objectId": target_chart_id,
                        "fields": "newPosition",
                        "newPosition": {
                            "overlayPosition": {
                                "anchorCell": {
                                    "sheetId": target_sheet_id,
                                    "rowIndex": 1,      # Row 2
                                    "columnIndex": 6    # Column G
                                },
                                "offsetXPixels": 50,
                                "widthPixels": calc_width,
                                "heightPixels": calc_height
                            }
                        }
                    }
                }]
            }
            
            service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=requests_body).execute()
            print(f"SUCCESS: Chart scaled to {calc_width}x{calc_height}")
        else:
            print("No chart found on Pivot sheet to resize.")

    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    extracted_data = fetch_data()
    update_maritime_system(extracted_data)
