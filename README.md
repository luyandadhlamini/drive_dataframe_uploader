# drive_dataframe_uploader
 Package **downloads**, **appends** or **replaces** a Google Sheet with a pandas dataframe.
 A dictionary is used to determine the sheet(s) to work on.

<details><summary><b>How To Use</b></summary>
 1. Install the module using pip:

    
    pip install drive-dataframe-uploader
    

2. Import the module & instantiate the GoogleDrivePython class:
    ```sh
    import drive-dataframe-uploader as ddu
    dl = ddu.DataFrameLoader()
    ```

3. Store the full path to your [google service account](https://cloud.google.com/iam/docs/service-accounts) in a variable:
    ```sh
    service_account = '/Users/luyanda.dhlamini/Projects/client_secret.json'
    ```

4. Create a dictionary with the Google Spreadsheet sheet name & doc key to work on:
    ```sh
    # Dictionary format: { sheet_name_string : { 'doc_key' : spread_sheet_id_string } }
    data_dictionary = { 'Sheet1' : { 'doc_key' : '1ZqLUWuANFa8rCRF2tpIgCNl-IWsmw2MKWxPVpmfUM0s' } }
    
    ```
 

5. Use the get_existing() function to download the data from a Spreadsheet into a pandas DataFrame:
    ```sh
    # Create & save a text file into the local directory
    existing_df = dl.get_existing( data_dictionary = data_dictionary, service_account_json_path = service_account)
    print( existing_df.head() )
    
    # Change the first value of the downloaded dataframe.
    existing_df.at[ 0, existing_df.columns[0] ] = 'Changed'
    
    # Store the dictionary in the data_dictionary
    data_dictionary['Sheet1']['dataframe'] = existing_df
    
    # Print the data_dictionary to assess if the dataframe has been stores in the data_dictionary
    print( data_dictionary )
    ```


6. Use the update_data_overview() function to upload & overwrite the data in a Spreadsheet using the data_dictionary created above. All tabular data contained in the target sheet is deleted:
    ```sh
    # Pass in the data_dictionary created above to overwrite data in the existing Sheet1.
    # If the passed in sheet cannot be found, a new sheet is created.
    data_dictionary2 = dl.update_data_overwrite( data_dictionary = data_dictionary, service_account_json_path = service_account) 
    
    ```
 
 7. Use the update_data_append() function to upload & append to the data in a Spreadsheet using the data_dictionary created above. All tabular data contained in the target sheet is first downloaded, empty rows & columns are removed. New data is concated vertically to existing data:
    ```sh
    # Pass in the data_dictionary created above to append to data in the existing Sheet1.
    # If the passed in sheet cannot be found, a new sheet is created.
    data_dictionary3 = dl.update_data_append( data_dictionary = data_dictionary, service_account_json_path = service_account)
    ```
 

</details>
 
<details><summary><b>Package Dependencies</b></summary>

* [pandas](https://pypi.org/project/pandas/)
* [gspread](https://pypi.org/project/gspread/)
* [gspread-dataframe](https://pypi.org/project/gspread-dataframe/)

</details>

