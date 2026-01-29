import smartsheet
import os
import pandas as pd


def get_sheets_in_folder(client, folder_id):
    """Retrieves all sheets inside a given Smartsheet folder and returns them as a list of dictionaries."""
    try:
        folder = client.Folders.get_folder(folder_id)
        if hasattr(folder, 'sheets'):
            sheets = folder.sheets
            sheet_info = [{"Sheet ID": sheet.id, "Sheet Name": sheet.name} for sheet in sheets]
            sheet_ids_list = [sheet.id for sheet in sheets]
            print(f"Found {len(sheets)} sheets in Folder ID {folder_id}.")
        for sheet in sheet_info:
            print(f"  - {sheet['Sheet Name']} (ID: {sheet['Sheet ID']})")
        return sheets, sheet_info, sheet_ids_list

    except smartsheet.exceptions.ApiError as e:
        print(f"Smartsheet API error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def save_sheet_ids_to_csv(folder_id, output_folder="sheet_id_exports"):
    """Extracts all sheet IDs from a Smartsheet folder and saves them as a CSV file."""
    try:
        os.makedirs(output_folder, exist_ok=True)
        sheets = get_sheets_in_folder(folder_id)

        if sheets:
            df = pd.DataFrame(sheets)
            csv_filename = f"{output_folder}/sheet_ids_{folder_id}.csv"
            df.to_csv(csv_filename, index=False, encoding="utf-8")

            print(f"Saved all Sheet IDs from Folder {folder_id} to {csv_filename}")
            return csv_filename
        else:
            print("No sheets found, skipping CSV creation.")
            return None

    except Exception as e:
        print(f"Error saving Sheet IDs to CSV: {e}")
        return None


if __name__ == "__main__":
    folder_id = 5778260903651204  # Replace with the actual folder ID
    print(get_sheets_in_folder(folder_id))
