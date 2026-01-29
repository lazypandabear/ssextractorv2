# Smartsheet to Google Drive (and AppSheet) Backup

Simple app to pull Smartsheet data, comments, and attachments, then place them into Google Drive (and optionally AppSheet). You fill a web form, click **Start Migration**, and the files land in your Drive folders.

## What it does
- Download each Smartsheet as Excel.
- Extract comments.
- Download row attachments.
- Upload everything to your Google Drive folders.
- (Optional) Send data to AppSheet.

## What you need
- Smartsheet account + API key.
- Google account and three Drive folder IDs (sheets, comments, attachments). Share those folders with your service account email if using a service account.
- One of:
  - Service account JSON (`service_account.json`) — default.
  - OAuth (`client_secret.json` + consent flow) if you prefer personal Google login.

## Quick setup
1) Create `.env` in the project folder (values can also be entered in the web form):
   ```
   SMARTSHEET_API_KEY=...
   SMARTSHEET_FOLDER_ID=...
   GOOGLE_DRIVE_SHEETS_FOLDER_ID=...
   GOOGLE_DRIVE__COMMENTS_FOLDER_ID=...
   GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID=...
   GOOGLE_AUTH_TYPE=service_account   # default; switch to oauth if needed
   GOOGLE_SERVICE_ACCOUNT_FILE=service_account.json
   GOOGLE_OAUTH_CLIENT_SECRET_FILE=client_secret.json
   GOOGLE_OAUTH_TOKEN_FILE=token.json
   ```
2) Place your `service_account.json` (or `client_secret.json`) in the project folder.
3) Install dependencies: `pip install -r requirements.txt`.

## Run
1) Start the web app: `python app/app.py`.
2) Open the URL shown (usually http://127.0.0.1:5000).
3) Fill the form:
   - Smartsheet API key and folder ID.
   - The three Google Drive folder IDs.
   - Choose auth method (service account is default; OAuth optional).
   - Upload a service account JSON (default) or use the placeholder Google sign-in button (future OAuth flow).
4) Click **Start Migration** and watch status.
5) Check Drive for new files.

## File map (what matters)
- `app/` - application code.
  - `app/app.py` - Flask web form + status.
  - `app/main.py` - runs the migration steps.
  - `app/ssextractor.py` - Smartsheet download, comment/attachment handling, Drive uploads (writes to `resource/`).
  - `app/config.py` - stores credentials (filled from `.env` and the form).
  - `app/process_state.py` - tracks status.
  - `app/getSsSheetID.py` - fetches sheet IDs in a Smartsheet folder.
- `resource/` - generated downloads (sheets, comments, row mappings, attachments).
- `backup/` - archived older scripts/configs.

## Tips if it fails
- 404/403 on Drive: the folder ID is wrong or not shared with the service account. Fix sharing or use OAuth.
- Missing files in Drive: ensure the three folder IDs are filled; leave parent blank if you don’t use it.
- OAuth issues: upload `client_secret.json`, allow consent, ensure `token.json` gets created.
- Attachments corrupted: fixed by not adding auth headers to the Smartsheet pre-signed URL (already in code).

## Notes
- Always keep `.env` and credential JSONs private.
- The dev server (`app.py`) is for local use only.
