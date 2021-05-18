import pandas as pd
import gspread
import gspread_dataframe

class DataFrameLoader(object):
    """ 
    Class uses a dictionary to download, append or replace Google Sheets with a pandas dataframe. 
    Requires pandas, gspread, gspread_dataframe.
    """ 
    
    def get_existing(self, data_dictionary, service_account_json_path, verbose = 0):
        """
        Function fetches data in google sheets spreadsheets & returns it as a DataFrames stores in data_dictionary.
        Parameters:
        ----------
        data_dictionary : dict
            A dictionary with the sheet_name(s) & document key(s).
            Format: {sheet_name_string : { 'doc_key': doc_key_string} }
        Returns:
        data_dictionary : dict
            Format: {sheet_name_string : { 'doc_key': doc_key_string, 'dataframe' : DataFrame } }        
        verbose : Object, default 0
            A value that determines if the funtion prints progress to stdout.
        """
        # set up google credentials to google sheet:
        gc = gspread.service_account(filename= service_account_json_path)

        for sheet_name in data_dictionary.keys():
            if verbose != 0:
                print("\nWorking on sheet:", sheet_name)
            google_credentials = gspread.service_account(filename= service_account_json_path)

            try:
                workbook = google_credentials.open_by_key(data_dictionary[sheet_name]['doc_key'])
                worksheet = workbook.worksheet(sheet_name)

                # read in existing data in that sheet into a pandas dataframe here in python:
                existing = gspread_dataframe.get_as_dataframe(worksheet)
                existing = existing.dropna(axis=0,how='all') # Remove empty rows
                existing = existing.dropna(axis=1,how='all') # Remove empty columns
                data_dictionary[sheet_name]['dataframe'] = existing

            except gspread.exceptions.WorksheetNotFound:
                # If the sheet cannot be found return a empty dataframe.
                data_dictionary[sheet_name]['dataframe'] = pd.DataFrame()

        return data_dictionary


    def update_data_overwrite(self, data_dictionary, service_account_json_path, verbose = 0):
        """
        Function writes data stored in a dictionary to a google sheets spreadsheet.
        Python modules from the gspread api are used to perform updates.
        Data that is already in the sheet is first cleared.
        Parameters
        ----------
        data_dictionary : dict
            A dictionary with the sheet_name(s), document key(s) & dataframe(s) to update in a spreadsheet.
            Format: {sheet_name_string : { 'doc_key': doc_key_string, 'dataframe' : DataFrame } }
        service_account_json_path : str
            A string with the full absolute path. to a service account json file with secret keys.
        verbose : Object, default 0
            A value that determines if the funtion prints progress to stdout.
        """
        # set up google credentials to google sheet:
        google_credentials = gspread.service_account(filename= service_account_json_path)

        for sheet_name in data_dictionary.keys():
            if verbose != 0:
                print("\nWorking on sheet:", sheet_name)
            try:
                workbook = google_credentials.open_by_key(data_dictionary[sheet_name]['doc_key'])
                worksheet = workbook.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                workbook.add_worksheet(sheet_name,rows=1000,cols=26)
                worksheet = workbook.worksheet(sheet_name)

            worksheet.clear()
            gspread_dataframe.set_with_dataframe(
                worksheet = worksheet, dataframe = data_dictionary[sheet_name]['dataframe'], row = 1, col = 1)
            if verbose != 0:
                print(sheet_name , "processed.")

        if verbose != 0:
            print("Process complete.")
    
        return data_dictionary

    def update_data_append(self, data_dictionary, service_account_json_path, verbose = 0):
        """
        Function writes data stored in a dictionary to a google sheets spreadsheet.
        Python modules from the gspread api are used to perform updates.
        Data that is already in the sheet is first cleared.
        Parameters
        ----------
        data_dictionary : dict
            A dictionary with the sheet_name(s), document key(s) & dataframe(s) to update 
                in a spreadsheet.
            Format: {sheet_name_string : { 'doc_key': doc_key_string, 'dataframe' : DataFrame } }
        service_account_json_path : str
            A string with the full absolute path. to a service account json file with secret keys.
        verbose : Object, default 0
            A value that determines if the funtion prints progress to stdout.
        """
        google_credentials = gspread.service_account(filename= service_account_json_path)

        for sheet_name in data_dictionary.keys():
            if verbose != 0:
                print("\nWorking on sheet:", sheet_name)

            try:
                workbook = google_credentials.open_by_key(data_dictionary[sheet_name]['doc_key'])
                worksheet = workbook.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                workbook.add_worksheet(sheet_name,rows=1000,cols=26)
                worksheet = workbook.worksheet(sheet_name)

            # read in existing data in that sheet into a pandas dataframe here in python:
            existing = gspread_dataframe.get_as_dataframe(worksheet)
            existing = existing.dropna(axis=0,how='all') # Remove empty rows
            existing = existing.dropna(axis=1,how='all') # Remove empty columns
            if verbose != 0:
                print("Existing dataframe size:", existing.shape[0])

            final_df = pd.concat([existing, data_dictionary[sheet_name]['dataframe']], axis=0, ignore_index=True)
            if verbose != 0:
                print("Merged dataframe size:", final_df.shape[0])

            worksheet.clear()
            gspread_dataframe.set_with_dataframe(
                worksheet = worksheet, dataframe = final_df, row = 1, col = 1)
            if verbose != 0:
                print(sheet_name , "processed.")

        if verbose != 0:
            print("Process complete.")
            
        return data_dictionary
