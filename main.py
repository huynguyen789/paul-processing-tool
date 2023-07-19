import camelot
import pandas as pd
import numpy as np
from IPython.display import display
import time
import re
from PyPDF2 import PdfReader
import os 
import multiprocessing


def read_pdf(file_path):
    # Read the PDF file using Camelot
    print(f"Start reading PDF file {file_path}...")
    tables = camelot.read_pdf(file_path, pages='all' )
    # clean_tables(tables)
    return tables

def clean_table(table):
    # print(f"\nStart cleaning table...")
    # Check if the table has more than 1 column
    if table.shape[1] > 1:
        # print(f'Table has {table.shape[1]} columns. Cleaning up...')
        # # Drop the first column
        table = table.drop(columns=0)
        # Reset the column names
        table.columns = range(table.shape[1])
        # print(f'Table now has {table.shape[1]} columns.')
    return table

def find_table_location(pdf_file, target_table):
    """
    Searches through a PDF file to find the page numbers 
    containing the specified table.

    Params:
    pdf_file: Path to PDF file
    target_table: Name of table to search for

    Returns:
    A list of page numbers where the target table is found.
    Includes multi-page tables.

    """
    
    print(f"Searching for table: {target_table}")
    
    reader = PdfReader(pdf_file)

    pages_with_table = []

    for page_num in range(len(reader.pages)):

        # Search each page's text for target table
        print(f"Searching page {page_num} out of {len(reader.pages)-1}")
        
        page = reader.pages[page_num]
        text = page.extract_text()
        
        if target_table in text:
            print(f"Found table on page {page_num}")
            pages_with_table.append(page_num)

            # Check for multi-page table
            if page_num < len(reader.pages):

                # If next page has numeric prefix, add it 
                next_page = reader.pages[page_num+1]  
                next_text = next_page.extract_text()

                if re.search(r'\d+\.\d+\.\d+', next_text):
                    pages_with_table.append(page_num + 2)

            # # Add offset for next table  
            # if pages_with_table:
            #     pages_with_table.append(pages_with_table[-1] + 2)

    return pages_with_table

def convert_to_ranges(numbers):
    """
    Convert a list of numbers into a list of ranges.
    """
    ranges = []
    
    for i in range(0, len(numbers), 2):
        
        if i < len(numbers) - 1:
            ranges.append(f"{numbers[i]}-{numbers[i+1]}")
        else:
            ranges.append(str(numbers[i]))

    return ranges

def display_tables(tables):
    # Set the max column width to a high number (e.g., 1000) to display long contents
    pd.set_option('display.max_colwidth', 1000)

    # Display all tables
    print(f"Displaying {len(tables)} tables:")
    for i, table in enumerate(tables):
        table_number = i + 1
        print(f"\nTable {table_number}")
        display(table.df)

def display_processed_tables(tables):
    # Set the max column width to a high number (e.g., 1000) to display long contents
    pd.set_option('display.max_colwidth', 1000)

    # Display all processed tables
    print(f"\nDisplaying {len(tables)} tables:")
    for i, df in enumerate(tables):
        print(f"Table {i + 1}")
        display(df)

def find_target_table(tables, desired_name):
    
    current_table = None
    desired_tables = []
    
    #Run through table_list
    for table in tables:
        first_cell = table.df.iloc[0,0].split('\n')[0]

        #If the cell in first row, first column has the desire format "6.x.x.x" AND the desire_name: Start a new table
        if first_cell.startswith("6.") and desired_name in first_cell:
            # Found start of new desired table
            
            if current_table is None:
                current_table = table.df.copy()
            
            # print(f"Found start of new desired table:")   
            # display(current_table)

        
        #if the table doesnt match the desire_format AND there is a current_table: concat this table into the current table:         
        elif current_table is not None and not table.df.iloc[0][1].startswith("6."):  
            # Continuation of previous desired table
            print(f"Found continuation of previous desired table: {table.df.iloc[0]}")
            print(f"test: {table.df.iloc[0][1]}")
            # print(f"Found continuous table, before cleaning:")   
            # display(table.df)
            #Clean table before concat:
            table.df = clean_table(table.df)
            # print(f"Continuous table after cleaning:")   
            # display(table.df)
            current_table = pd.concat([current_table, table.df])

            # print(f"Table after concat:")
            # display(current_table)
            
        else: 
            #else: return the current table, and reset the current_table to None
            print(f"\nSkipping a none-desired table: {first_cell}")
            if current_table is not None:
                # Save the current table
                desired_tables.append(current_table)
                #Reset, mark end of the desired table
                current_table = None       
            
    return desired_tables

def process_tables(tables):  

    processed_tables = []
    
    for i, table in enumerate(tables):
        # print(f"\nProcessing table {i+1}...")
        # print(f"Table before processing:")
        # display(table)

    
        # Split the first cell of the first row and use it as column headers
        first_row = table.iloc[0, 0]
        headers = first_row.split('\n')  
        # print(f"First row: {first_row}")
        # print(f"Headers: {headers}")
        # headers.append("MissingHeader")
        table.columns = headers
        
        # Extract the table name from the first row
        table_name = table.iloc[0, 0].split('\n')[0]

        
        # Extract Band info from column 2
        # print(f"Extracting Band info...")
        line2 = table.iloc[1,0]
        # print(f"line2: {line2}")
        if 'Band' in line2:
            band_info = line2.split(' ')
            for word in band_info:
                if word.startswith('Band'):
                    band_num = word[4:]  # Extract everything after "Band"
                    table.insert(1, 'Band', band_num)
                    break
            else:
                print(f"No 'Band' keyword in line: {line2}")
         

        # print("Table after header:")
        # display(table)
          
        # # Check if the expected columns are in the table
        # if 'Unit' not in table.columns:
        #     print(f"Table {i+1} doesn't have the expected structure. Skipping...")
        #     continue
  
        # Extract Testname, ULCH, BW, MOD, RD info
        # print(f"Extracting Testname, ULCH, BW, MOD, RD info...")
        # Define patterns
        testname_pattern = r"(.*):@"
        ulch_pattern = r"ULCH: (\d+),"
        bw_pattern = r"BW: ([\d\.]+ MHz)"
        mod_pattern = r"UL_MOD_RB: ([^,]+),"
        rd_pattern = r"UL_MOD_RB: [^,]+, (.*)"

        # Extract info
        table['Testname'] = table.iloc[:,0].str.extract(testname_pattern)
        table['ULCH'] = table.iloc[:,0].str.extract(ulch_pattern)
        table['BW'] = table.iloc[:,0].str.extract(bw_pattern)
        table['MOD'] = table.iloc[:,0].str.extract(mod_pattern)
        table['RD'] = table.iloc[:,0].str.extract(rd_pattern)
        
        # print(f"Table before split Unit column:")
        # display(table)
   
        # Split 'Measured' and 'Unit' columns 
        # Create a temporary DataFrame for the split results
        split_df = table['Unit'].str.split(expand=True)

        # Assign the split results to 'Measured' and 'Unit' only where there are values
        table.loc[split_df[0].notna(), 'Measured'] = split_df.loc[split_df[0].notna(), 0]
        table.loc[split_df[1].notna(), 'Unit'] = split_df.loc[split_df[1].notna(), 1]

        # Drop the first column
        table.drop(table.columns[0], axis=1, inplace=True)

        # Create a new column filled with the table name
        table.insert(0, 'Table Name', table_name)
        table = table.iloc[1:]
        
        # Reorganize the columns
        # print(f"Reorganizing columns...")
        new_column_order = ['Table Name', 'Testname', 'Band', 'ULCH', 'BW', 'MOD', 'RD', 'Limit Low', 'Limit High', 'Measured', 'Unit', 'Status']
        table = table.reindex(columns=new_column_order)

        # Drop rows with NaN values in Testname and Band columns
        table = table.dropna(subset=['Testname'], how='all')


        # Reset the index
        table.reset_index(drop=True, inplace=True)
        
        #Append processed table to processed_tables:
        processed_tables.append(table)
        
        
    return processed_tables


#UI TKINTER:
import tkinter as tk
from tkinter import filedialog

# Add a GUI file picker
root = tk.Tk()
root.withdraw()
file_path = filedialog.askopenfilename()

pdf_file = file_path
# Extract name without extension
file_name = os.path.splitext(os.path.basename(pdf_file))[0]

# "LTE_3GPP_v15_r8_FDD_FORD_All_TEMPS_TCU2_5_ROW_012023_5GSIM_AT_2023-06-28_16-27-57_188.pdf"
#  "25C_DATA_Extract(1temperatureOnly).pdf"
# "300_pages_extract.pdf"

target_table = "6.2.2 Maximum Output Power" 
# pdf_file = "300_pages_extract.pdf"
# Extract name without extension
file_name = os.path.splitext(os.path.basename(pdf_file))[0]


# Find target table pages
pages = find_table_location(pdf_file, target_table)

# Format page numbers for camelot
page_ranges = convert_to_ranges(pages)
print(f"Pages to read: {page_ranges}")


clean_tables = []
total_ranges = len(page_ranges)


for i, page_range in enumerate(page_ranges):
    
    print(f"\nProcessing {i+1}/{total_ranges}: {page_range}")
    # Calculate the estimate time remaining based on the progress so far
    progress = i / total_ranges
    time_remaining = (total_ranges - i) * 9 / 60
    print(f"Estimated time remaining: {time_remaining:.2f} minutes")
    
    start_time = time.time()
    # Read all tables from pdf
    long_tables = camelot.read_pdf(pdf_file, pages=page_range, backend="poppler")
    end_time = time.time()
    print(f"Time taken to load table: {end_time - start_time:.2f} seconds")
    
    start_time = time.time()
    desire_tables = find_target_table(long_tables, target_table)
    # display_processed_tables(desire_tables)
    end_time = time.time()
    print(f"Time taken to find target table: {end_time - start_time:.2f} seconds")
   
    
    start_time = time.time()
    processed_table = process_tables(desire_tables)
    end_time = time.time()
    print(f"Time taken to process tables: {end_time - start_time:.2f} seconds")
    # display_processed_tables(processed_table)
    
    
    #append to clean_tables:
    clean_tables.extend(processed_table)


# Concatenate all the dataframes in the list into a single dataframe
if not clean_tables:
    print("No tables extracted")
else: 
    all_tables = pd.concat(clean_tables, ignore_index=True)
    display(all_tables.head(10)) # Display the first few rows of the resulting dataframe
    # Save to Excel using file name
    excel_file = f"final_{file_name}.xlsx" 
    all_tables.to_excel(excel_file, index=False)

    #Auto open folder:
    import subprocess
    import platform
    folder_path = os.path.dirname(excel_file)

    if platform.system() == 'Windows':
        os.startfile(folder_path)

    elif platform.system() == 'Darwin': 
        subprocess.Popen(['open', folder_path])

    else:
        subprocess.Popen(['xdg-open', folder_path])
        


