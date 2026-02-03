# config.py
import os
import contextvars

try:
    from dotenv import load_dotenv

    load_dotenv(override=True)
except Exception:
    # If python-dotenv is not installed, we simply skip loading .env
    pass

CREDENTIALS = {
    "SMARTSHEET_API_KEY": os.getenv("SMARTSHEET_API_KEY"),
    "SMARTSHEET_FOLDER_ID": os.getenv("SMARTSHEET_FOLDER_ID"),
    "GOOGLE_DRIVE_PARENT_FOLDER_ID": os.getenv("GOOGLE_DRIVE_PARENT_FOLDER_ID"),
    "GOOGLE_DRIVE_SHEETS_FOLDER_ID": os.getenv("GOOGLE_DRIVE_SHEETS_FOLDER_ID"),
    "GOOGLE_DRIVE__COMMENTS_FOLDER_ID": os.getenv("GOOGLE_DRIVE__COMMENTS_FOLDER_ID"),
    "GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID": os.getenv("GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID"),
    "APPSHEET_API_KEY": os.getenv("APPSHEET_API_KEY"),
    "APPSHEET_APP_ID": os.getenv("APPSHEET_APP_ID"),
    "APPSHEET_TABLE_NAME": os.getenv("APPSHEET_TABLE_NAME"),
    "SMARTSHEET_BASE_DIR": os.getenv("SMARTSHEET_BASE_DIR"),
    # Google auth configuration
    # GOOGLE_AUTH_TYPE: "service_account" (default) or "oauth"
    "GOOGLE_AUTH_TYPE": os.getenv("GOOGLE_AUTH_TYPE", "service_account"),
    # File names can be overridden if the files are stored elsewhere
    "GOOGLE_SERVICE_ACCOUNT_FILE": os.getenv(
        "GOOGLE_SERVICE_ACCOUNT_FILE",
        os.getenv("SERVICE_ACCOUNT_FILE", "SmartSheetDataArchive.json"),
    ),
    "GOOGLE_OAUTH_CLIENT_SECRET_FILE": os.getenv("GOOGLE_OAUTH_CLIENT_SECRET_FILE", "client_secret.json"),
    "GOOGLE_OAUTH_TOKEN_FILE": os.getenv("GOOGLE_OAUTH_TOKEN_FILE", "token.json"),
}

_CREDENTIALS_CTX = contextvars.ContextVar("credentials_override", default=None)


def get_credentials():
    creds = _CREDENTIALS_CTX.get()
    return creds if creds is not None else CREDENTIALS


def get_credential(key, default=None):
    return get_credentials().get(key, default)


def set_thread_credentials(creds):
    return _CREDENTIALS_CTX.set(creds)


def reset_thread_credentials(token):
    _CREDENTIALS_CTX.reset(token)
