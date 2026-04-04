import os
import requests
import gspread
import json
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz
import traceback

# --- CONFIGURATION ---
# Target Google Sheet and Tab Names
SPREADSHEET_ID = '1a0NUGV_PngH8sO2ZoqoiqGyAEKwgCcvK04B2Gpu4g7Q'
DATA_SHEET_NAME = 'Wavetable'
PIVOT_SHEET_NAME = 'Wavetable-pivotInfinite'

# Timezone set to Melbourne
MELB_TZ = pytz.timezone('Australia/Melbourne')

# PORT PHILLIP BAY NODES
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
    """Extracts wave data from JSON endpoints."""
    now_melb = datetime.now(MELB_TZ)
    ext_date = now_melb.strftime("%d/%m/%Y")
    ext_time = now_melb.strftime("%H:%M")
    
    rows_to_append = []
    for node in NODES:
        obs_date, obs_time, obs_timestamp = "N/A", "N/A", "N/A"
        sig_wave, peak_period, peak_direction, wind_spd, wind_dir = 0.0, 0.0, 0.0, 0.0, 0.0
        node_display_name = node["name"]

        try:
            response = requests.get(node["url"], headers=HEADERS, timeout=15)
            if response.status_code == 200:
                payload_list = response.json().get("data", [])
                if payload_list:
                    latest = payload_list[0]
                    dt_melb = datetime.fromtimestamp(int(latest["time"]), pytz.utc).astimezone(MELB_TZ)
                    
                    obs_date = dt_melb.strftime("%Y-%m-%d")
                    obs_time = dt_melb.strftime("%H:%M")
                    obs_timestamp = dt_melb.strftime("%Y-%m-%d %H:%M")
                    
                    sig_wave = float(latest.get("hsig", 0))
                    peak_period = float(latest.get("tp", 0))
                    peak_direction = float(latest.get("tpdeg", 0))
                    wind_spd = float(latest.get("windspeed", 0))
                    wind_dir = float(latest.get("winddirect", 0))
            else:
                node_display_name += f" (Status {response.status_code})"
        except Exception:
            node_display_name += " (Fetch Error)"

        rows_to_append.append([
            obs_date, obs_time, obs_timestamp, node_display_name,
            sig_wave, peak_period, peak_direction, wind_spd,
            "", wind_dir, ext_date, ext_time, f"{ext_date} {ext_time}"
        ])
    return rows_to_append

def update_maritime_system(data):
    """Pushes data and resizes chart using Raw JSON String to bypass library auto-correction."""
    creds_raw = os.environ.get('GOOGLE_CREDS')
    if not creds_raw or not data:
        print("Missing Credentials or Data.")
        return

    try:
        # 1. AUTHENTICATION & TOKEN GENERATION
        creds_info = json.loads(creds_raw)
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        
        # Manually refresh the token for the raw REST call
        import google.auth.transport.requests
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        access_token = creds.token
        
        # Gspread for Row Insertion
        client = gspread.authorize(creds)
        ss = client.open_by_key(SPREADSHEET_ID)
        data_sheet = ss.worksheet(DATA_SHEET_NAME)
        pivot_sheet = ss.worksheet(PIVOT_SHEET_NAME)
        
        # 2. INSERT RAW DATA
        data_sheet.insert_rows(data, row=2, value_input_option='USER_ENTERED')
        print(f"SUCCESS: {len(data)} rows pushed to {DATA_SHEET_NAME}")

        # 3. FETCH METADATA FOR CHART & SHEET IDs
        meta_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}"
        meta_res = requests.get(meta_url, headers={"Authorization": f"Bearer {access_token}"})
        spreadsheet = meta_res.json()
        
        target_sheet_id = None
        target_chart_id = None
        
        for s in spreadsheet['sheets']:
            if s['properties']['title'] == PIVOT_SHEET_NAME:
                target_sheet_id = s['properties']['sheetId']
                if 'charts' in s and len(s['charts']) > 0:
                    target_chart_id = s['charts'][0]['chartId']

        if target_chart_id:
            # 4. CALCULATE DYNAMIC WIDTH (+50% Wider)
            pivot_rows = pivot_sheet.get_values("A:A")
            last_row_count = len([r for r in pivot_rows if r and r[0]])
            
            calc_width = int((last_row_count * 68) + 250)
            if calc_width < 1200: calc_width = 1200
            
            # 5. THE RAW PAYLOAD (Hard-coded strings to bypass library snake_case conversion)
            # Double curly braces are required for literal braces in f-strings.
            raw_payload = f"""
            {{
                "requests": [{{
                    "updateEmbeddedObjectPosition": {{
                        "objectId": {target_chart_id},
                        "fields": "newPosition",
                        "newPosition": {{
                            "overlayPosition": {{
                                "anchorCell": {{
                                    "sheetId": {target_sheet_id},
                                    "rowIndex": 1,
                                    "columnIndex": 6
                                }},
                                "offsetXPixels": 50,
                                "widthPixels": {calc_width},
                                "heightPixels": 585
                            }}
                        }}
                    }}
                }}]
            }}
            """

            batch_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}:batchUpdate"
            print(f"Sending BatchUpdate for Chart ID: {target_chart_id}...")
            
            response = requests.post(
                batch_url,
                headers={{
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json
