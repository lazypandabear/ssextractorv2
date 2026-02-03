import re
import os
import pandas as pd
import requests
import glob  # Used for wildcard search
import smartsheet
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
#from dotenv import load_dotenv
import time  # For sleep
import contextvars
import process_state
import config
from pathlib import Path

# Project paths (resource folder holds generated downloads)
DEFAULT_BASE_DIR = r"\\relprdvintfs01.convergeict.com\Smartsheet$"

_GOOGLE_CTX = contextvars.ContextVar("google_services_ctx", default=None)

def get_base_dir() -> Path:
    base_dir_raw = config.get_credential("SMARTSHEET_BASE_DIR") or DEFAULT_BASE_DIR
    if os.name != "nt" and base_dir_raw.startswith("\\\\"):
        raise RuntimeError(
            "UNC path configured on non-Windows host. Mount the share and set "
            "SMARTSHEET_BASE_DIR to the mount path (e.g., /mnt/smartsheet)."
        )
    return Path(base_dir_raw)

def get_resource_root():
    api_key = config.get_credential("SMARTSHEET_API_KEY")
    user_suffix = api_key[-6:] if api_key and len(api_key) >= 6 else "default"
    job_id = config.get_credential("JOB_ID")
    if job_id:
        return get_base_dir() / "resource" / user_suffix / job_id
    return get_base_dir() / "resource" / user_suffix

# If you still need .env for other non-SMARTSHEET values, you can load it.
#load_dotenv(override=True)
# Use the credentials from the global config
#SMARTSHEET_API_KEY = config.CREDENTIALS["SMARTSHEET_API_KEY"]
#GOOGLE_DRIVE_SHEETS_FOLDER_ID = config.CREDENTIALS["GOOGLE_DRIVE_SHEETS_FOLDER_ID"]
#GOOGLE_DRIVE__COMMENTS_FOLDER_ID = config.CREDENTIALS["GOOGLE_DRIVE__COMMENTS_FOLDER_ID"]
#GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID = config.CREDENTIALS["GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID"]
#APPSHEET_API_KEY = config.CREDENTIALS["APPSHEET_API_KEY"]
#APPSHEET_APP_ID = config.CREDENTIALS["APPSHEET_APP_ID"]
#APPSHEET_TABLE_NAME = config.CREDENTIALS["APPSHEET_TABLE_NAME"]

# Google API Credentials
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
drive_service = None
sheet_service = None
google_credentials = None
DEFAULT_SERVICE_ACCOUNT_FILE = "SmartSheetDataArchive.json"

def _get_google_auth_setting(key, default):
    value = config.get_credential(key)
    return value if value else default

def _load_user_credentials(client_secret_file, token_file):
    """Handles OAuth2 installed-app flow and token refresh/persistence."""
    creds = None
    if os.path.exists(token_file):
        creds = UserCredentials.from_authorized_user_file(token_file, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            creds = flow.run_local_server(port=0, prompt="consent")
        with open(token_file, "w") as token:
            token.write(creds.to_json())
    return creds

def get_google_services():
    """
    Returns Drive and Sheets service clients using either service account
    credentials or OAuth2 user credentials based on config.
    """
    ctx = _GOOGLE_CTX.get()
    current_creds_id = id(config.get_credentials())
    if ctx and ctx.get("creds_id") == current_creds_id:
        try:
            if not ctx["google_credentials"].valid:
                ctx["google_credentials"].refresh(Request())
            return ctx["drive_service"], ctx["sheet_service"], ctx["google_credentials"]
        except Exception:
            ctx = None
            _GOOGLE_CTX.set(None)

    auth_type = (_get_google_auth_setting("GOOGLE_AUTH_TYPE", "service_account")).lower()
    service_account_file = _get_google_auth_setting("GOOGLE_SERVICE_ACCOUNT_FILE", DEFAULT_SERVICE_ACCOUNT_FILE)
    client_secret_file = _get_google_auth_setting("GOOGLE_OAUTH_CLIENT_SECRET_FILE", "client_secret.json")
    token_file = _get_google_auth_setting("GOOGLE_OAUTH_TOKEN_FILE", "token.json")

    if auth_type == "service_account":
        if not os.path.exists(service_account_file):
            raise FileNotFoundError(f"Service account file not found: {service_account_file}")
        google_credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    elif auth_type == "oauth":
        google_credentials = _load_user_credentials(client_secret_file, token_file)
    else:
        raise ValueError("GOOGLE_AUTH_TYPE must be 'service_account' or 'oauth'.")

    if not google_credentials.valid:
        google_credentials.refresh(Request())

    drive_service = build("drive", "v3", credentials=google_credentials)
    sheet_service = build("sheets", "v4", credentials=google_credentials)
    _GOOGLE_CTX.set(
        {
            "creds_id": current_creds_id,
            "drive_service": drive_service,
            "sheet_service": sheet_service,
            "google_credentials": google_credentials,
        }
    )
    return drive_service, sheet_service, google_credentials

def describe_drive_item(item_id, label):
    """Log Drive item metadata to confirm shared-drive vs My Drive."""
    try:
        drive_service, _, _ = get_google_services()
        meta = drive_service.files().get(
            fileId=item_id,
            fields="id,name,parents,driveId,trashed",
            supportsAllDrives=True,
        ).execute()
        drive_id = meta.get("driveId")
        print(
            f"Drive item {label}: id={meta.get('id')} name={meta.get('name')} "
            f"driveId={drive_id} parents={meta.get('parents')} trashed={meta.get('trashed')}"
        )
        if not drive_id:
            print(f"Warning: {label} is not in a Shared Drive (driveId is empty).")
    except Exception as e:
        print(f"Failed to describe Drive item {label} ({item_id}): {e}")

def get_smartsheet_client():
    import config
    api_key = config.get_credential("SMARTSHEET_API_KEY")
    #print("DEBUG: API Key is:", api_key)  # This should print the key entered by the user
    if not api_key:
        raise ValueError("No API key provided. Please update config.CREDENTIALS.")
    return smartsheet.Smartsheet(api_key)

def access_config_file(key):
    import config
    config_value = config.get_credential(key)
    return config_value
    

# Initialize Smartsheet Client
#smartsheet_client = get_smartsheet_client()

# Ensure folder exists
def ensure_folder(folder_path):
    """Ensures a folder exists before saving files."""
    os.makedirs(folder_path, exist_ok=True)

def ensure_resource_subdir(base_path: Path, sheet_id=None, create=True) -> str:
    """Return a resource subfolder path (optionally per-sheet)."""
    folder = base_path / str(sheet_id) if sheet_id is not None else base_path
    if create:
        folder.mkdir(parents=True, exist_ok=True)
    return str(folder)

def sheet_folder_path(sheet_id):
    return ensure_resource_subdir(get_resource_root() / "sheets", sheet_id)

def comments_folder_path(sheet_id):
    return ensure_resource_subdir(get_resource_root() / "comments", sheet_id)

def row_mapping_folder_path(sheet_id):
    return ensure_resource_subdir(get_resource_root() / "row_mapping", sheet_id)

def attachments_folder_path(sheet_id, create=True):
    return ensure_resource_subdir(get_resource_root() / "attachments", sheet_id, create=create)

def prune_empty_dirs(base_folder: str) -> None:
    """Remove empty row subfolders and then the base folder if it becomes empty."""
    if not base_folder or not os.path.exists(base_folder):
        return
    if not os.path.isdir(base_folder):
        return
    # Remove empty row subfolders
    for entry in os.listdir(base_folder):
        entry_path = os.path.join(base_folder, entry)
        if os.path.isdir(entry_path) and not os.listdir(entry_path):
            os.rmdir(entry_path)
    # Remove base folder if it's now empty
    if not os.listdir(base_folder):
        os.rmdir(base_folder)

def sanitize_filename(filename, max_length=100):
    """
    Removes or replaces invalid characters and truncates long filenames.
    """
    import re
    # Remove invalid characters
    filename = re.sub(r'[<>:"/\\|?*\t]', '_', filename)
    # Remove excessive whitespace and commas
    filename = re.sub(r'[,\s]+', '_', filename).strip('_')
    # Truncate if too long (preserve file extension if present)
    if len(filename) > max_length:
        base, ext = os.path.splitext(filename)
        filename = base[:max_length - len(ext)] + ext
    return filename

def format_row_id(value):
    """Return row ID as a string to avoid Excel numeric precision loss."""
    if pd.isna(value):
        return ""
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)

def download_smartsheet_as_excel(sheet_id):
    """Downloads a Smartsheet as an Excel file and adds the Row ID column efficiently."""
    smartsheet_client = get_smartsheet_client()
    try:
        # Define folders and paths under resource/sheets
        sheet_folder = sheet_folder_path(sheet_id)

        # Download Excel and save it
        excel_data = smartsheet_client.Sheets.get_sheet_as_excel(sheet_id, sheet_folder)
        excel_data.save_to_file()  # Save file in the directory
        latest_file = wait_for_excel_file(sheet_folder, retries=10, delay=1)
        if latest_file:
            target_path = os.path.join(sheet_folder, f"{sheet_id}.xlsx")
            if os.path.abspath(latest_file) != os.path.abspath(target_path):
                os.replace(latest_file, target_path)
                print(f"Renamed Excel file to {target_path}")
        print(f"Smartsheet {sheet_id} downloaded")
        return None

    except Exception as e:
        print(f"Error downloading Smartsheet {sheet_id}: {e}")
        return None
    

def wait_for_excel_file(sheet_folder, retries=100, delay=2):
    """Waits until the Excel file appears in the specified folder."""
    while retries > 0:
        excel_files = glob.glob(os.path.join(sheet_folder, "*.xlsx"))
        if excel_files:
            excel_files.sort(key=os.path.getmtime, reverse=True)
            return excel_files[0]  # Return the most recent file
        print(" Waiting for Excel file to be available...")
        time.sleep(delay)
        retries -= 1
    print(f" No Excel file found in {sheet_folder} after waiting.")
    return None

def fetch_smartsheet_row_ids(sheet_id):
    """Fetches all row IDs from Smartsheet and returns a row number to row ID mapping."""
    try:
        smartsheet_client = get_smartsheet_client()
        sheet_data = smartsheet_client.Sheets.get_sheet(sheet_id)
        row_mapping = {row.row_number: row.id for row in sheet_data.rows}  # Map row number → row ID

        print(f" Retrieved {len(row_mapping)} Smartsheet row IDs for Sheet {sheet_id}")
        return row_mapping

    except Exception as e:
        print(f" Error fetching Smartsheet row IDs for {sheet_id}: {e}")
        return {}
    

# Extract & Store Comments
def extract_and_store_comments(sheet_id):
    """Reads Smartsheet Excel, extracts comments, and stores them row-wise."""
    try:
        # Find the downloaded Excel file
        sheet_folder = sheet_folder_path(sheet_id)
        excel_files = glob.glob(os.path.join(sheet_folder, "*.xlsx"))
        if not excel_files:
            print(f"Smartsheet Excel not found in {sheet_folder}")
            return

        excel_path = excel_files[0]
        comments_folder = comments_folder_path(sheet_id)

        original_file = wait_for_excel_file(sheet_folder, retries=100, delay=2)  # Use the first (and only) file

        # Load Excel into Pandas safely
        with pd.ExcelFile(original_file, engine="openpyxl") as xls:
            df_comments = pd.read_excel(xls, sheet_name="Comments", header=None)

        # Dynamically assign headers (Fixes length mismatch error)
        expected_columns = ["Relative Row", "Comments", "Created By", "Created On", "Actual Row ID"]
        df_comments = df_comments.iloc[:, :len(expected_columns)]  # Trim extra columns
        df_comments.columns = expected_columns[:df_comments.shape[1]]  # Assign only existing columns
        df_comments = df_comments.dropna(how='all')
        df_comments['Relative Row']= df_comments['Relative Row'].ffill()
        if "Actual Row ID" in df_comments.columns:
            df_comments["Actual Row ID"] = df_comments["Actual Row ID"].map(format_row_id)
        df_comments.to_excel(f"{comments_folder}/{sheet_id}_comments.xlsx", index=False)



        print(f"Saved comments to {comments_folder}/{sheet_id}_comments.xlsx")

    except Exception as e:
        print(f"Error extracting comments for Sheet {sheet_id}: {e}")



def create_relative_row_mapping(sheet_id):
    """Creates a mapping table of 'Relative Row' to 'Actual Row ID' from Smartsheet comments data."""
    try:
        # Find the downloaded Smartsheet Excel file
        sheet_folder = sheet_folder_path(sheet_id)
        mapping_folder = row_mapping_folder_path(sheet_id)
        excel_files = glob.glob(os.path.join(sheet_folder, "*.xlsx"))

        if not excel_files:
            print(f"Smartsheet Excel not found in {sheet_folder}")
            return None

        original_file = wait_for_excel_file(sheet_folder, retries=100, delay=2)

        # Load Excel into Pandas Safely
        with pd.ExcelFile(original_file, engine="openpyxl") as xls:
            df_comments = pd.read_excel(xls, sheet_name="Comments", header=None)

        if "Comments" not in xls.sheet_names:
            print(f"No 'Comments' sheet found in {sheet_folder}")
            return None
        
        if df_comments.empty:
            print(f"No comments found in 'Comments' sheet for {sheet_id}.")
            return None

        # Assign headers dynamically (Handle missing headers)
        expected_columns = ["Relative Row", "Comments", "Created By", "Created On", "Actual Row ID"]
        df_comments = df_comments.iloc[:, :len(expected_columns)]  # Trim extra columns
        df_comments.columns = expected_columns[:df_comments.shape[1]]  # Assign headers

        # Fetch Smartsheet row IDs from API
        row_mapping = fetch_smartsheet_row_ids(sheet_id)
    

        # Extract numeric row numbers from "Relative Row"
        df_comments["Relative Row"] = df_comments["Relative Row"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")


        # Map "Relative Row" to "Actual Row ID" using Smartsheet row numbers
        df_comments["Actual Row ID"] = df_comments["Relative Row"].map(row_mapping).map(format_row_id)

        # Create a dictionary mapping "Relative Row" to "Actual Row ID"
        mapping_table = df_comments.set_index("Relative Row")["Actual Row ID"].to_dict()

        # Convert to DataFrame
        df_mapping = pd.DataFrame(mapping_table.items(), columns=["Relative Row", "Row ID"])
        df_mapping["Row ID"] = df_mapping["Row ID"].map(format_row_id)

        # Save to file
        mapping_path = os.path.join(mapping_folder, f"{sheet_id}_relative_row_mapping.xlsx")
        df_mapping.to_excel(mapping_path, index=False)

        print(f" Created Relative Row → Row ID mapping table: {mapping_path}")
        return df_mapping

    except Exception as e:
        print(f" Error creating mapping table for Sheet {sheet_id}: {e}")
        return None
    


def prepare_sheet_for_drive_upload(sheet_id):
    """Adds Row ID and Filename columns to the downloaded Excel file for Google Drive upload."""
    original_file = None
    try:
        smartsheet_client = get_smartsheet_client()
        # ? Define folders and paths
        sheet_folder = sheet_folder_path(sheet_id)

        original_file = wait_for_excel_file(sheet_folder, retries=100, delay=2)  # Use the first (and only) file
        if not original_file:
            print(f"No Excel file found for sheet {sheet_id} to prepare for Drive upload.")
            return None

        # ? Load Excel into Pandas Safely
        with pd.ExcelFile(original_file, engine="openpyxl") as xls:
            sheet_name = xls.sheet_names[0]  # Assume first sheet contains data
            df = pd.read_excel(xls, sheet_name=sheet_name)

        # ? Fetch Smartsheet Row IDs in bulk (Efficient)
        sheet_data = smartsheet_client.Sheets.get_sheet(sheet_id)

        row_ids = [format_row_id(row.id) for row in sheet_data.rows]
        if len(row_ids) < len(df):
            row_ids.extend([""] * (len(df) - len(row_ids)))
        df["Row ID"] = row_ids[:len(df)]

        # ? Add "Filename" column
        df["Filename"] = f"{sheet_id}.xlsx"

        # ? Save the updated file
        updated_excel_path = os.path.join(sheet_folder, f"{sheet_id}.xlsx")
        df.to_excel(updated_excel_path, index=False)

        # ? Delete the original downloaded file after modification
        if os.path.exists(original_file) and os.path.abspath(original_file) != os.path.abspath(updated_excel_path):
            os.remove(original_file)
        print(f"Replaced original Excel file with {updated_excel_path}")
        return updated_excel_path, original_file

    except Exception as e:
        print(f"Error preparing Excel for Google Drive upload for sheet {sheet_id}: {e}")
        # Fallback: attempt to rename the original download so it is still usable
        try:
            if original_file and os.path.exists(original_file):
                fallback_path = os.path.join(sheet_folder_path(sheet_id), f"{sheet_id}.xlsx")
                os.replace(original_file, fallback_path)
                print(f"Fallback: renamed {original_file} to {fallback_path} after error.")
                return fallback_path, original_file
        except Exception as rename_err:
            print(f"Fallback rename failed for sheet {sheet_id}: {rename_err}")


def merge_comments_with_row_mapping(sheet_id):
    """Merges the comments table with row mapping table using wildcard search."""
    try:
        # Define the folder path
        comments_folder = comments_folder_path(sheet_id)
        row_mapping_folder = row_mapping_folder_path(sheet_id)
        
        # Find the comments file using wildcard
        comments_files = glob.glob(os.path.join(comments_folder, f"{sheet_id}*_comments.xlsx"))
        mapping_files = glob.glob(os.path.join(row_mapping_folder, f"{sheet_id}*_relative_row_mapping.xlsx"))
        
        if not comments_files or not mapping_files:
            print(f"Comments or mapping file not found in {comments_folder} or in {row_mapping_folder}")
            return None
        
        comments_file = comments_files[0]
        mapping_file = mapping_files[0]
        
        # Load the comments and mapping data
        df_comments = pd.read_excel(comments_file)
        df_mapping = pd.read_excel(mapping_file)
        
        # Ensure correct column names before merging
        df_comments.rename(columns={
            df_comments.columns[0]: "Relative Row",
            df_comments.columns[1]: "Comments",
            df_comments.columns[2]: "Created By",
            df_comments.columns[3]: "Created On"
        }, inplace=True)
        df_comments["Relative Row"] = df_comments["Relative Row"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
      
        
        df_mapping.rename(columns={
            df_mapping.columns[0]: "Relative Row",
            df_mapping.columns[1]: "Row ID"
        }, inplace=True)
        df_mapping['Relative Row'] = df_mapping['Relative Row'].astype("Int64")
        df_mapping["Row ID"] = df_mapping["Row ID"].map(format_row_id)
        
        # Merge comments with row mapping
        df_merged = df_comments.merge(df_mapping, on="Relative Row", how="left")
        
        # Add Sheet ID column
        df_merged.insert(0, "Sheet ID", sheet_id)
        
        # Save the updated comments table
        merged_file_path = os.path.join(comments_folder, f"{sheet_id}_comments.xlsx")
        df_merged.to_excel(merged_file_path, index=False)
        
        print(f"Merged comments saved: {merged_file_path}")
        return merged_file_path
    except Exception as e:
        print(f"Error merging comments with row mapping for {sheet_id}: {e}")
        return None

def get_or_create_drive_folder(folder_name, parent_folder_id):
    """Checks if a folder exists in Google Drive, creates it if not, and returns its ID."""
    try:
        drive_service, _, _ = get_google_services()

        if not parent_folder_id:
            raise ValueError(f"Missing parent folder ID for '{folder_name}'.")

        describe_drive_item(parent_folder_id, f"parent for {folder_name}")
        print(f"Searching for Drive folder '{folder_name}' under parent {parent_folder_id}")
        query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder'"
        results = drive_service.files().list(
            q=query,
            fields="files(id)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()

        if results.get("files"):
            return results["files"][0]["id"]  # ? Return existing folder ID

        # ? Create folder if it doesn't exist
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        file_metadata["parents"] = [parent_folder_id]
        print(f"Creating Drive folder '{folder_name}' under parent {parent_folder_id}")
        print(f"Creating Drive folder metadata: name={folder_name} parent={parent_folder_id}")
        folder = drive_service.files().create(
            body=file_metadata,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        describe_drive_item(folder["id"], f"created folder {folder_name}")
        return folder["id"]

    except HttpError as e:
        print(f"Drive folder create failed for {folder_name}: {e}")
        try:
            print(f"Drive error details: {e.error_details}")
        except Exception:
            pass
        raise
    except Exception as e:
        print(f"Error creating Google Drive folder {folder_name}: {e}")
        return None


def upload_to_google_drive(sheet_id):
    """Uploads an Excel file to Google Drive in sheets/{sheet_id} folder."""
    try:
        drive_service, _, _ = get_google_services()
        # Find the downloaded Excel file using wildcard
        sheet_folder = sheet_folder_path(sheet_id)
        excel_files = glob.glob(os.path.join(sheet_folder, "*.xlsx"))
        if not excel_files:
            print(f"Smartsheet Excel not found in {sheet_folder}")
            return None

        file_path = excel_files[0]  # Select first found file
        GOOGLE_DRIVE_SHEETS_FOLDER_ID = config.get_credential("GOOGLE_DRIVE_SHEETS_FOLDER_ID")
        # Ensure `sheets/{sheet_id}` folder exists in Google Drive
        print(f"Using Sheets parent folder ID: {GOOGLE_DRIVE_SHEETS_FOLDER_ID}")
        drive_sheet_folder_id = get_or_create_drive_folder(str(sheet_id), GOOGLE_DRIVE_SHEETS_FOLDER_ID)

        if not drive_sheet_folder_id:
            print(f"Failed to create/find folder in Google Drive for Sheet {sheet_id}")
            return None

        # Upload the file to `sheets/{sheet_id}` folder in Drive
        file_metadata = {
            "name": os.path.basename(file_path),
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "parents": [drive_sheet_folder_id],
        }
        media = MediaFileUpload(file_path, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        print(f"Uploading sheet to Drive: {file_path} parent={drive_sheet_folder_id}")
        file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()

        print(f"Uploaded {file_path} to Google Drive folder: sheets/{sheet_id} (parent {drive_sheet_folder_id})")
        return file.get("id")

    except HttpError as e:
        print(f"Drive upload failed for {file_path}: {e}")
        try:
            print(f"Drive error details: {e.error_details}")
        except Exception:
            pass
        return None
    except Exception as e:
        print(f"Error uploading {file_path} to Google Drive: {e}")
        return None
    
def download_smartsheet_attachments(sheet_id):
    smartsheet_client = get_smartsheet_client()
    """Downloads all attachments from a Smartsheet and saves them in resource/attachments/{sheet_id}/{row_id}/"""

    try:
        print(f"Starting download of attachments for sheet {sheet_id}")
        # Create base folder for the sheet's attachments
        base_folder = attachments_folder_path(sheet_id, create=False)

        # Get all rows in the sheet
        sheet_data = smartsheet_client.Sheets.get_sheet(sheet_id)

        attachments_saved = False
        for row in sheet_data.rows:
            # Check for cancellation before processing a new row
            if process_state.is_cancel_requested():
                print("Cancellation requested before processing row; stopping attachments download.")
                return

            row_id = row.id  # Unique Row ID in Smartsheet
            row_folder = os.path.join(base_folder, str(row_id))

            # Get all attachments for this row
            attachments = smartsheet_client.Attachments.list_row_attachments(sheet_id, row_id).data
            if not attachments:
                continue

            row_folder_created = False

            for attachment in attachments:
                # Check for cancellation before processing each attachment
                if process_state.is_cancel_requested():
                    print(f"Cancellation requested; stopping download for row {row_id}.")
                    return

                att_id = attachment.id
                file_name = sanitize_filename(attachment.name)  # Clean the filename
                file_path = os.path.join(row_folder, file_name)

                # Fetch attachment details
                retrieve_att = smartsheet_client.Attachments.get_attachment(sheet_id, att_id)
                file_url = retrieve_att.url  # Check if it's downloadable

                if file_url:
                    # Smartsheet returns a pre-signed URL; adding Authorization breaks S3 downloads
                    response = requests.get(file_url, stream=True, allow_redirects=True)
                    if response.status_code != 200:
                        print(f"Skipped {file_name}: download returned {response.status_code} ({response.text[:200]})")
                        continue
                    if not row_folder_created:
                        os.makedirs(row_folder, exist_ok=True)  # Create folder for row only when saving a file
                        row_folder_created = True
                    with open(file_path, "wb") as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            # Check for cancellation during file download
                            if process_state.is_cancel_requested():
                                print(f"Cancellation requested during download of {file_path}; stopping file download.")
                                return
                            file.write(chunk)
                    print(f"Downloaded: {file_path}")
                    attachments_saved = True
                else:
                    print(f"Skipped (No download link): {file_name}")

        if not attachments_saved:
            prune_empty_dirs(base_folder)

        print(f"Completed downloading all attachments for sheet {sheet_id}")

    except Exception as e:
        print(f"Error downloading attachments for sheet {sheet_id}: {e}")


def upload_comments_to_drive(sheet_id):
    """Uploads the comments Excel file to Google Drive inside comments/{sheet_id}/."""
    try:
        drive_service, _, _ = get_google_services()
        GOOGLE_DRIVE__COMMENTS_FOLDER_ID = config.get_credential("GOOGLE_DRIVE__COMMENTS_FOLDER_ID")
        print(f"Using Comments parent folder ID: {GOOGLE_DRIVE__COMMENTS_FOLDER_ID}")
        # Define the comments folder path
        comments_folder = comments_folder_path(sheet_id)

        # Find the comments Excel file using wildcard (*.xlsx)
        excel_files = glob.glob(os.path.join(comments_folder, "*.xlsx"))
        if not excel_files:
            print(f"No comments file found in {comments_folder} for upload.")
            return None

        file_path = excel_files[0]  # Use the first (and only) found file

        # ? Ensure Drive folder exists for comments
        drive_folder_id = get_or_create_drive_folder(f"{sheet_id}", GOOGLE_DRIVE__COMMENTS_FOLDER_ID)

        # ? Upload the file to Google Drive
        file_metadata = {
            "name": os.path.basename(file_path),
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "parents": [drive_folder_id],
        }
        media = MediaFileUpload(file_path, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        print(f"Uploading comments to Drive: {file_path} parent={drive_folder_id}")
        file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()

        print(f"Uploaded {file_path} to Google Drive in comments/{sheet_id}/ (parent {drive_folder_id})")
        return file.get("id")

    except HttpError as e:
        print(f"Drive upload failed for comments {file_path}: {e}")
        try:
            print(f"Drive error details: {e.error_details}")
        except Exception:
            pass
        return None
    except Exception as e:
        print(f"Error uploading comments for sheet {sheet_id} to Google Drive: {e}")
        return None


def upload_attachments_to_drive(sheet_id):
    """Uploads all attachments in resource/attachments/{sheet_id}/{row_id}/ to Google Drive."""
    try:
        drive_service, _, _ = get_google_services()
        GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID = config.get_credential("GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID")
        print(f"Using Attachments parent folder ID: {GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID}")
        # Define the base attachments directory
        attachments_folder = attachments_folder_path(sheet_id, create=False)
        if not os.path.exists(attachments_folder):
            print(f"No attachments found for sheet {sheet_id}.")
            return None

        # Ensure Drive folder exists for attachments/{sheet_id}
        drive_sheet_folder_id = get_or_create_drive_folder(f"{sheet_id}", GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID)

        uploaded_files = {}

        # Loop through row_id folders
        for row_folder in os.listdir(attachments_folder):
            row_folder_path = os.path.join(attachments_folder, row_folder)
            if not os.path.isdir(row_folder_path):
                continue  # Skip non-folder files

            # Find all files inside row_id folder
            attachment_files = glob.glob(os.path.join(row_folder_path, "*.*"))
            if not attachment_files:
                # Skip creating Drive folders for rows with no attachments
                continue

            # Ensure Drive folder exists for attachments/{sheet_id}/{row_id}
            drive_row_folder_id = get_or_create_drive_folder(row_folder, drive_sheet_folder_id)
            for file_path in attachment_files:
                file_name = os.path.basename(file_path)

                # Upload the file to Google Drive
                file_metadata = {
                    "name": file_name,
                    "mimeType": "application/octet-stream",
                    "parents": [drive_row_folder_id],
                }
                media = MediaFileUpload(file_path, mimetype="application/octet-stream")
                print(f"Uploading attachment to Drive: {file_path} parent={drive_row_folder_id}")
                file = drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields="id",
                    supportsAllDrives=True,
                ).execute()
                drive_link = f"https://drive.google.com/file/d/{file.get('id')}/view"

                # Store uploaded file info
                uploaded_files[file_name] = drive_link

                print(f"Uploaded {file_name} to Google Drive in attachments/{sheet_id}/{row_folder}/ (parent {drive_row_folder_id})")

        return uploaded_files

    except HttpError as e:
        print(f"Drive upload failed for attachments under sheet {sheet_id}: {e}")
        try:
            print(f"Drive error details: {e.error_details}")
        except Exception:
            pass
        return None
    except Exception as e:
        print(f"Error uploading attachments for sheet {sheet_id}: {e}")
        return None


# Send Data to AppSheet
def send_data_to_appsheet_database(google_sheet_id, sheet_name):
    """Fetches data from Google Sheets and sends it to AppSheet Database."""
    try:
        _, _, google_creds = get_google_services()
        APPSHEET_API_KEY = config.get_credential("APPSHEET_API_KEY")
        APPSHEET_APP_ID = config.get_credential("APPSHEET_APP_ID")
        APPSHEET_TABLE_NAME = config.get_credential("APPSHEET_TABLE_NAME")
        # Fetch Google Sheets Data (Ensuring it remains an Excel file)
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{google_sheet_id}/values/{sheet_name}!A1:Z1000"
        headers = {"Authorization": f"Bearer {google_creds.token}"}
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f" Failed to fetch Google Sheet data: {response.text}")
            return

        sheet_data = response.json().get("values", [])
        if not sheet_data:
            print(" No data found in Google Sheet.")
            return

        # Format Data for AppSheet
        headers = sheet_data[0]
        rows_data = sheet_data[1:]

        records = []
        for row in rows_data:
            record = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
            records.append(record)

        # Send to AppSheet
        appsheet_url = f"https://api.appsheet.com/api/v2/apps/{APPSHEET_APP_ID}/tables/{APPSHEET_TABLE_NAME}/Action"
        payload = {"Action": "AddOrUpdate", "Properties": {"Locale": "en-US"}, "Rows": records}
        appsheet_headers = {"Content-Type": "application/json", "ApplicationAccessKey": APPSHEET_API_KEY}

        response = requests.post(appsheet_url, headers=appsheet_headers, json=payload)
        if response.status_code == 200:
            print(f" Successfully synced data with AppSheet.")
        else:
            print(f" Failed to sync with AppSheet: {response.text}")
    except Exception as e:
        print(f" Error syncing with AppSheet: {e}")

if __name__ == "__main__":
# **Main Execution**
    sheet_id = 457130802210692  # Replace with actual Smartsheet ID
    download_smartsheet_as_excel(sheet_id)
    extract_and_store_comments(sheet_id)
    create_relative_row_mapping(sheet_id)
    merge_comments_with_row_mapping(sheet_id)
    download_smartsheet_attachments(sheet_id)
    prepare_sheet_for_drive_upload(sheet_id)
    upload_to_google_drive(sheet_id)
    upload_comments_to_drive(sheet_id)
    upload_attachments_to_drive(sheet_id)
