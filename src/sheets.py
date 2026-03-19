import json
import os
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

from .config import GOOGLE_CREDS_JSON, SHEET_ID, get_sheet_columns

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_gspread_client():
    """Authenticate and return a gspread client.

    Supports two modes:
    1. GOOGLE_CREDS_JSON points to a file path  -> load from file
    2. GOOGLE_CREDS_BASE64 env var set           -> decode from base64 (GitHub Actions)
    """
    base64_creds = os.getenv("GOOGLE_CREDS_BASE64")

    if base64_creds:
        import base64
        creds_dict = json.loads(base64.b64decode(base64_creds).decode("utf-8"))
        credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        creds_path = Path(GOOGLE_CREDS_JSON)
        if not creds_path.exists():
            raise FileNotFoundError(
                f"Google credentials file not found: {creds_path}\n"
                "Set GOOGLE_CREDS_JSON in .env or GOOGLE_CREDS_BASE64 for CI."
            )
        credentials = Credentials.from_service_account_file(str(creds_path), scopes=SCOPES)

    return gspread.authorize(credentials)


def _ensure_worksheet(spreadsheet, tab_name, columns):
    """Get or create a worksheet tab with the given columns as header."""
    try:
        ws = spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=len(columns))
        ws.append_row(columns, value_input_option="RAW")
        print(f"  Created new tab: {tab_name}")
    return ws


def _sanitize_tab_name(name):
    """Google Sheets tab names: max 100 chars, no special chars."""
    clean = name.replace("@", "").replace("/", "_").replace("\\", "_")
    return clean[:100]


def _get_existing_msg_ids(worksheet, msg_id_col_index):
    """Get all existing message IDs from a worksheet to avoid duplicates."""
    try:
        all_values = worksheet.col_values(msg_id_col_index + 1)  # 1-indexed
        # Skip header row, convert to set of strings
        return set(str(v) for v in all_values[1:] if v)
    except Exception:
        return set()


def push_to_sheets(all_rows):
    """Push scraped rows to Google Sheets.

    Creates:
    1. "All Data" tab - contains all messages from all channels
    2. Individual channel tabs - one tab per channel
    
    Rows are appended (not overwritten).
    Duplicates are automatically skipped based on msg_id.
    """
    if not all_rows:
        print("No rows to push.")
        return

    if not SHEET_ID:
        raise ValueError("SHEET_ID is not set. Set it in .env or as an environment variable.")

    client = get_gspread_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    columns = get_sheet_columns()
    
    # Find msg_id column index for duplicate checking
    msg_id_col_index = columns.index("msg_id") if "msg_id" in columns else None
    post_link_col_index = columns.index("post_link") if "post_link" in columns else None

    # ═══════════════════════════════════════════════════════════════
    # 1. Push ALL data to "All Data" tab (with duplicate check)
    # ═══════════════════════════════════════════════════════════════
    all_data_tab = "All Data"
    ws_all = _ensure_worksheet(spreadsheet, all_data_tab, columns)
    
    # Get existing post_links to check for duplicates (more unique than msg_id alone)
    existing_links = set()
    if post_link_col_index is not None:
        try:
            existing_links = set(ws_all.col_values(post_link_col_index + 1)[1:])
        except Exception:
            pass
    
    # Filter out duplicates
    new_rows = []
    for row in all_rows:
        post_link = row.get("post_link", "")
        if post_link and post_link not in existing_links:
            new_rows.append(row)
            existing_links.add(post_link)  # Track newly added ones too
    
    skipped_count = len(all_rows) - len(new_rows)
    if skipped_count > 0:
        print(f"  Skipping {skipped_count} duplicate messages (already in sheet)")
    
    if not new_rows:
        print("  No new messages to push (all duplicates).")
        return
    
    # Build all rows in column order
    all_batch = []
    for row in new_rows:
        all_batch.append([str(row.get(col, "")) for col in columns])
    
    # Append in chunks of 500 (Sheets API limit)
    chunk_size = 500
    for i in range(0, len(all_batch), chunk_size):
        chunk = all_batch[i:i + chunk_size]
        ws_all.append_rows(chunk, value_input_option="RAW")
        print(f"  {all_data_tab}: appended {len(chunk)} rows")

    # ═══════════════════════════════════════════════════════════════
    # 2. Push to individual channel tabs (with duplicate check)
    # ═══════════════════════════════════════════════════════════════
    channels = {}
    for row in new_rows:
        ch_key = row.get("username", "unknown")
        if ch_key not in channels:
            channels[ch_key] = []
        channels[ch_key].append(row)

    for ch_name, rows in channels.items():
        tab_name = _sanitize_tab_name(ch_name)
        ws = _ensure_worksheet(spreadsheet, tab_name, columns)

        # Build rows in column order
        batch = []
        for row in rows:
            batch.append([str(row.get(col, "")) for col in columns])

        # Append in chunks of 500
        for i in range(0, len(batch), chunk_size):
            chunk = batch[i:i + chunk_size]
            ws.append_rows(chunk, value_input_option="RAW")
            print(f"  {tab_name}: appended {len(chunk)} rows")

    print(f"\nTotal pushed to Google Sheets: {len(new_rows)} new rows")
    print(f"  - 'All Data' tab: {len(new_rows)} rows")
    print(f"  - Individual tabs: {len(channels)} channels")
    if skipped_count > 0:
        print(f"  - Skipped duplicates: {skipped_count} rows")
