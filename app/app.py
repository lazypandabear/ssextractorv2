import warnings
# Suppress FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from flask import Flask, render_template, request, jsonify
import logging
from logging.handlers import RotatingFileHandler
import threading
import main
import process_state
import config
import os
from werkzeug.utils import secure_filename
from ssextractor import get_or_create_drive_folder

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = os.getcwd()

logger = logging.getLogger("smartsheet_migrator")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    log_path = os.path.join(app.config["UPLOAD_FOLDER"], "app.log")
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=3)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(file_handler)
    logger.addHandler(logging.StreamHandler())

def log(message):
    logger.info(message)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        log("Received migration request.")
        # Reset cancel flag for each new migration
        process_state.cancel_requested = False
        if process_state.migration_status.get('running'):
            log("Migration request rejected; migration already running.")
            return render_template(
                'index.html',
                error_message="Migration already running. Please wait for completion.",
            )
        
        # Get configuration from form
        configuration = {
            "SMARTSHEET_API_KEY": request.form.get('smartsheet_api_key'),
            "SMARTSHEET_FOLDER_ID": request.form.get('smartsheet_folder_id'),
            "GOOGLE_DRIVE_PARENT_FOLDER_ID": request.form.get('google_drive_parent_folder_id'),
            "GOOGLE_DRIVE_SHEETS_FOLDER_ID": request.form.get('google_drive_sheets_folder_id'),
            "GOOGLE_DRIVE__COMMENTS_FOLDER_ID": request.form.get('google_drive_comments_folder_id'),
            "GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID": request.form.get('google_drive_attachments_folder_id'),
            "GOOGLE_AUTH_TYPE": request.form.get('google_auth_type') or config.CREDENTIALS.get("GOOGLE_AUTH_TYPE"),
        }

        # Update global configuration, but do not overwrite existing values with None/empty strings
        for key, value in configuration.items():
            if value:
                config.CREDENTIALS[key] = value

        # Handle OAuth client secret upload (required when using OAuth)
        oauth_client_upload = request.files.get('google_oauth_client_secret_upload')
        if oauth_client_upload and oauth_client_upload.filename:
            target_name = secure_filename("client_secret.json")
            upload_path = os.path.join(app.config["UPLOAD_FOLDER"], target_name)
            oauth_client_upload.save(upload_path)
            config.CREDENTIALS["GOOGLE_OAUTH_CLIENT_SECRET_FILE"] = target_name

        # Optional: accept an uploaded OAuth token file
        oauth_token_upload = request.files.get('google_oauth_token_upload')
        if oauth_token_upload and oauth_token_upload.filename:
            target_name = secure_filename("token.json")
            upload_path = os.path.join(app.config["UPLOAD_FOLDER"], target_name)
            oauth_token_upload.save(upload_path)
            config.CREDENTIALS["GOOGLE_OAUTH_TOKEN_FILE"] = target_name

        # Validate required fields before creating Drive folders.
        required_fields = {
            "SMARTSHEET_API_KEY": config.CREDENTIALS.get("SMARTSHEET_API_KEY"),
            "SMARTSHEET_FOLDER_ID": config.CREDENTIALS.get("SMARTSHEET_FOLDER_ID"),
            "GOOGLE_DRIVE_PARENT_FOLDER_ID": config.CREDENTIALS.get("GOOGLE_DRIVE_PARENT_FOLDER_ID"),
        }
        missing = [k for k, v in required_fields.items() if not v]
        if missing:
            log(f"Missing required fields: {', '.join(missing)}")
            return render_template('index.html', error_message=f"Missing required fields: {', '.join(missing)}")

        # Auto-create Drive subfolders under the parent.
        parent_id = config.CREDENTIALS.get("GOOGLE_DRIVE_PARENT_FOLDER_ID")
        try:
            config.CREDENTIALS["GOOGLE_DRIVE_SHEETS_FOLDER_ID"] = get_or_create_drive_folder("sheets", parent_id)
            config.CREDENTIALS["GOOGLE_DRIVE__COMMENTS_FOLDER_ID"] = get_or_create_drive_folder("comments", parent_id)
            config.CREDENTIALS["GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID"] = get_or_create_drive_folder("attachments", parent_id)
        except Exception as e:
            log(f"Failed to initialize Drive subfolders under parent {parent_id}: {e}")
            return render_template(
                "index.html",
                error_message=(
                    "Google Drive parent folder not found or inaccessible. "
                    "Use a folder ID inside a Shared Drive and ensure the service account "
                    "has access."
                ),
            )

        #print(config.CREDENTIALS)
        # Start migration in a background thread
        log("Starting migration thread.")
        threading.Thread(target=main.run_migration, name="migration-thread", daemon=True).start()
        return render_template('migration_started.html')
    return render_template('index.html')

@app.route('/status', methods=['GET'])
def status():
    # Return current migration status as JSON
    return jsonify(process_state.migration_status)

@app.route('/cancel', methods=['POST'])
def cancel():
    process_state.cancel_requested = True
    return jsonify({"status": "cancelled"})

if __name__ == '__main__':
    from waitress import serve
    log("Starting production server on port 5000...")
    serve(app, host='0.0.0.0', port=5000)
