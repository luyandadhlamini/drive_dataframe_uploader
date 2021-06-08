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
        service_account_json_path : str
            A string with the full absolute path. to a service account json file with secret keys.

        Returns:
        ----------
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

    def add_to_update_range(dataframe, additional_rows):
        """
        Function expands a dataframe to an ideal shape by adding a series of NoneType values to the end of 
        the existing dataframe.
        
        Parameters:
        ----------
        dataframe - DataFrame
            A dataframe to convert to an ideal shape
        additional_rows - int
            The number of rows shape to add to the end of a dataframe.
        
        Returns
        ----------
        result_df - DataFrame
            A converted dataframe.
        """
        
        dataframe_shape = dataframe.shape
        rows_outstanding = additional_rows - dataframe_shape[0]
        if rows_outstanding <=0: # If there 
            result_df = dataframe
        else:        
            none_shape = [[None]*dataframe.shape[1]]*rows_outstanding

            none_shape_df = pd.DataFrame(none_shape, columns= dataframe.columns)

            retult_df = dataframe.append( none_shape_df ).reset_index( drop = True )
        
        return retult_df

    def duplicate_update_from_template( data_dictionary, service_account_json_path, verbose = 0 ):
        """
        Function duplicates & overwrites a specific section of a template sheet with data provided in a input.
        If the given sheet name to write the duplicate to already exists, it is deleted & created a new.
        
        Parameters
        ----------
        data_dictionary - dict
            A dictionary with sheets to update & the data to update with.
            Format: {sheet_name_string : { 'doc_key': doc_key_string, 'dataframe' : DataFrame,
                    'template_sheet_name':'Template','update_range':'A2:J1551','additional_rows':1550} }
                sheet_name_string - a string with the name to call the newly created duplicate by.
                doc_key - a Google Spreadsheet file id, found in the files link.
                dataframe - a Pandas DataFrame object to overwrite the template with.
                template_sheet_name - a string with the name of the sheet to duplicate/ use as template
                update-range - a spreadhseet range to overwrite with the values in dataframe
                additional_rows - a integer used internally to add padding to the dataframe so that the update 
                    range & dataframe have similar dimentions. See function 'add_to_update_range'.
        service_account_json_path - str
            A string with the full absolute path. to a service account json file with secret keys.
        verbose - int, Object. Default 0
            A value that determines whether progress is printed during function execution.
        
        Returns
        ----------
        result - Bool
            A boollean with the value of True if the duplicate & uodating was successful.
            False is any exception were raised during execution.
        """
        
        google_credentials = gspread.service_account(filename= service_account_json_path)

        for sheet_name in data_dictionary.keys():
            if verbose != 0:
                print("\nWorking on sheet:", sheet_name)

            template_sheet_name = data_dictionary[sheet_name]['template_sheet_name']
            new_sheet_name = sheet_name
            try:
                workbook = google_credentials.open_by_key(data_dictionary[sheet_name]['doc_key'])        
                template_sheet = workbook.worksheet(template_sheet_name)

                # Check if a sheet existis with the same name as new_sheet_name 
                sheet_exists = [True for sheet in workbook.worksheets() if sheet.title == new_sheet_name]
                if len(sheet_exists) >0:
                    workbook.del_worksheet( workbook.worksheet(new_sheet_name) )

                new_sheet = workbook.duplicate_sheet(source_sheet_id=template_sheet.id,new_sheet_name=new_sheet_name)
                if verbose != 0:
                    print('Created new sheet:', new_sheet_name)

                update_range = data_dictionary[sheet_name]['update_range']
                dataframe = data_dictionary[sheet_name]['dataframe']

                dataframe = add_to_update_range( data_dictionaryz[sheet_name]['dataframe'],
                        data_dictionaryz[sheet_name]['additional_rows'])

                # Fill Nans with an empty string. 
                # Turn all values to lists.
                new_values_list = dataframe.fillna('').values.tolist()

                new_sheet.batch_update([ {'range' : update_range, 'values' : new_values_list} ])

                if verbose != 0:
                    print('Overwriting fixture details was successful.')
                result = True
            except gspread.exceptions.WorksheetNotFound:
                print("Template sheet not found.")
                result = False
            except:
                print('Operation not successful.')
                result = False
            
        return result 