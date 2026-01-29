import os
import logging
import time
import process_state
from ssextractor import (
    download_smartsheet_as_excel,
    extract_and_store_comments,
    create_relative_row_mapping,
    merge_comments_with_row_mapping,
    download_smartsheet_attachments,
    prepare_sheet_for_drive_upload,
    upload_to_google_drive,
    upload_comments_to_drive,
    upload_attachments_to_drive,
    access_config_file,
    get_smartsheet_client
)
from getSsSheetID import get_sheets_in_folder
import config

logger = logging.getLogger("smartsheet_migrator")

def log(message):
    logger.info(message)



def run_migration():
    """
    Runs the migration process using configuration from the form.
    """
    try:
        # Set initial process state
        log("Migration started.")
        process_state.migration_status['running'] = True
        process_state.migration_status['progress'] = 'Starting migration'
        process_state.migration_status['details'] = ''

        # Update environment variables for other credentials if needed.
        # ...
        # Create the Smartsheet client using the API key provided from the form.
        # Use the credentials from the global config

        #smartsheet_api_key = config.get("SMARTSHEET_FOLDER_ID")
        client = get_smartsheet_client()

        #client = smartsheet.Smartsheet()
        smartsheet_folder_id = access_config_file("SMARTSHEET_FOLDER_ID")
        # Get sheets in the specified Smartsheet folder
        sheets_data = get_sheets_in_folder(client, smartsheet_folder_id)
        if not sheets_data:
            process_state.migration_status['progress'] = 'Error retrieving sheets'
            process_state.migration_status['running'] = False
            return "Error: Could not retrieve sheets from folder. Please verify your API key and folder ID."

        sheets, sheet_info, sheet_ids_list = sheets_data
        process_state.migration_status['progress'] = f"Found {len(sheets)} sheets in folder ID {smartsheet_folder_id}."
        log(f"Found {len(sheets)} sheets in folder {smartsheet_folder_id}.")

        # Process each sheet
        for sheet in sheets:
            if process_state.cancel_requested:
                process_state.migration_status['progress'] = 'Migration Cancelled'
                process_state.migration_status['running'] = False
                return "Migration Cancelled by User"

            sheet_id = sheet.id
            process_state.migration_status['progress'] = f"Processing sheet {sheet_id}..."
            log(f"Processing sheet {sheet_id}.")

            download_smartsheet_as_excel(sheet_id)
            if process_state.cancel_requested: break

            extract_and_store_comments(sheet_id)
            if process_state.cancel_requested: break

            create_relative_row_mapping(sheet_id)
            if process_state.cancel_requested: break

            merge_comments_with_row_mapping(sheet_id)
            if process_state.cancel_requested: break

            download_smartsheet_attachments(sheet_id)
            if process_state.cancel_requested: break

            prepare_sheet_for_drive_upload(sheet_id)
            if process_state.cancel_requested: break

            upload_to_google_drive(sheet_id)
            if process_state.cancel_requested: break

            upload_comments_to_drive(sheet_id)
            if process_state.cancel_requested: break

            upload_attachments_to_drive(sheet_id)
            if process_state.cancel_requested: break

            # Optional: simulate delay between processing sheets
            time.sleep(1)

        if process_state.cancel_requested:
            process_state.migration_status['progress'] = 'Migration Cancelled'
            process_state.migration_status['running'] = False
            return "Migration Cancelled by User"

        process_state.migration_status['progress'] = "Migration Completed"
        process_state.migration_status['running'] = False
        print("🎉 Migration Completed Successfully!")
        return "Migration Completed Successfully!"
    except Exception as exc:
        process_state.migration_status['progress'] = "Migration Failed"
        process_state.migration_status['details'] = str(exc)
        process_state.migration_status['running'] = False
        logger.exception("Migration failed with an unhandled exception.")
        return f"Migration Failed: {exc}"


# Flask app to handle user input and display migration status
if __name__ == '__main__':
    SMARTSHEET_API_KEY = config.CREDENTIALS["SMARTSHEET_API_KEY"]
    SMARTSHEET_FOLDER_ID = config.CREDENTIALS["SMARTSHEET_FOLDER_ID"]
    GOOGLE_DRIVE_SHEETS_FOLDER_ID = config.CREDENTIALS["GOOGLE_DRIVE_SHEETS_FOLDER_ID"]
    GOOGLE_DRIVE__COMMENTS_FOLDER_ID = config.CREDENTIALS["GOOGLE_DRIVE__COMMENTS_FOLDER_ID"]
    GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID = config.CREDENTIALS["GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID"]
    #APPSHEET_API_KEY = config.CREDENTIALS["APPSHEET_API_KEY"]
    #APPSHEET_APP_ID = config.CREDENTIALS["APPSHEET_APP_ID"]
    #APPSHEET_TABLE_NAME = config.CREDENTIALS["APPSHEET_TABLE_NAME"]
    configuration = {
        "smartsheet_api_key": SMARTSHEET_API_KEY,
        "smartsheet_folder_id": SMARTSHEET_FOLDER_ID,
        "google_drive_sheets_folder_id": GOOGLE_DRIVE_SHEETS_FOLDER_ID,
        "google_drive_comments_folder_id": GOOGLE_DRIVE__COMMENTS_FOLDER_ID,
        "google_drive_attachments_folder_id": GOOGLE_DRIVE_ATTACHMENTS_FOLDER_ID
    }
    print("run_migration(configuration)")
    run_migration()
