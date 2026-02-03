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



def run_migration(job_id, job_credentials):
    """
    Runs the migration process using configuration from the form.
    """
    try:
        # Set initial process state
        log("Migration started.")
        process_state.update_status(job_id, running=True, progress="Starting migration", details="")

        # Update environment variables for other credentials if needed.
        # ...
        # Create the Smartsheet client using the API key provided from the form.
        # Use the credentials from the global config

        #smartsheet_api_key = config.get("SMARTSHEET_FOLDER_ID")
        token = config.set_thread_credentials(job_credentials)
        job_token = process_state.set_current_job(job_id)
        client = get_smartsheet_client()

        #client = smartsheet.Smartsheet()
        smartsheet_folder_id = access_config_file("SMARTSHEET_FOLDER_ID")
        # Get sheets in the specified Smartsheet folder
        sheets_data = get_sheets_in_folder(client, smartsheet_folder_id)
        if not sheets_data:
            process_state.update_status(job_id, running=False, progress="Error retrieving sheets")
            return "Error: Could not retrieve sheets from folder. Please verify your API key and folder ID."

        sheets, sheet_info, sheet_ids_list = sheets_data
        process_state.update_status(
            job_id,
            progress=f"Found {len(sheets)} sheets in folder ID {smartsheet_folder_id}.",
        )
        log(f"Found {len(sheets)} sheets in folder {smartsheet_folder_id}.")

        # Process each sheet
        for sheet in sheets:
            if process_state.is_cancel_requested():
                process_state.update_status(job_id, running=False, progress="Migration Cancelled", finished=True)
                return "Migration Cancelled by User"

            sheet_id = sheet.id
            process_state.update_status(job_id, progress=f"Processing sheet {sheet_id}...")
            log(f"Processing sheet {sheet_id}.")

            download_smartsheet_as_excel(sheet_id)
            if process_state.is_cancel_requested():
                break

            extract_and_store_comments(sheet_id)
            if process_state.is_cancel_requested():
                break

            create_relative_row_mapping(sheet_id)
            if process_state.is_cancel_requested():
                break

            merge_comments_with_row_mapping(sheet_id)
            if process_state.is_cancel_requested():
                break

            download_smartsheet_attachments(sheet_id)
            if process_state.is_cancel_requested():
                break

            prepare_sheet_for_drive_upload(sheet_id)
            if process_state.is_cancel_requested():
                break

            upload_to_google_drive(sheet_id)
            if process_state.is_cancel_requested():
                break

            upload_comments_to_drive(sheet_id)
            if process_state.is_cancel_requested():
                break

            upload_attachments_to_drive(sheet_id)
            if process_state.is_cancel_requested():
                break

            # Optional: simulate delay between processing sheets
            time.sleep(1)

        if process_state.is_cancel_requested():
            process_state.update_status(job_id, running=False, progress="Migration Cancelled", finished=True)
            return "Migration Cancelled by User"

        process_state.update_status(job_id, running=False, progress="Migration Completed", finished=True)
        print("🎉 Migration Completed Successfully!")
        return "Migration Completed Successfully!"
    except Exception as exc:
        process_state.update_status(job_id, running=False, progress="Migration Failed", details=str(exc), finished=True)
        logger.exception("Migration failed with an unhandled exception.")
        return f"Migration Failed: {exc}"
    finally:
        if 'job_token' in locals():
            process_state.reset_current_job(job_token)
        if 'token' in locals():
            config.reset_thread_credentials(token)


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
    job_id = process_state.create_job()
    job_credentials = dict(config.CREDENTIALS)
    job_credentials["JOB_ID"] = job_id
    run_migration(job_id, job_credentials)
