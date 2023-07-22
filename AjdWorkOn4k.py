from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor
import camelot
import pandas as pd
import numpy as np
from IPython.display import display
import time
import re
from PyPDF2 import PdfReader
import os 
import tkinter as tk
from tkinter import filedialog
import subprocess
import platform



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

def find_table_location(pdf_file, target_table):
    print(f"Searching for table: {target_table}")
    
    reader = PdfReader(pdf_file)

    pages_with_table = []
    cont_table = False
    for page_num in range(len(reader.pages)):

        # Search each page's text for target table
        print(f"Searching page {page_num+1} out of {len(reader.pages)}")
        
        page = reader.pages[page_num]
        text = page.extract_text()
        
        # If target table is found, add the page number to the list, start a continuous table
        if target_table in text:
            table_name = re.search(target_table, text).group(0)
            print(f"Start new target table '{table_name}' on page {page_num+1}")
            cont_table = True
            pages_with_table.append(page_num+1)
        #elif next page has 6.x.x.x pattern, end of continuous table, add the page number to the list
        elif re.search(r'\d+\.\d+\.\d+', text) and cont_table == True:
            print(f"End of continuous table on page {page_num}")
            cont_table = False
            pages_with_table.append(page_num)
        else:
            # print(f"Continuous table on page {page_num}")
            pass

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

def find_target_table(tables, desired_name):
    
    current_table = None
    desired_tables = []
    
    #Run through table_list
    for table in tables:
        first_cell = table.df.iloc[0,0].split('\n')[0]
        # print(f"\nChecking table: {first_cell}")
        #If the cell in first row, first column has the desire format "6.x.x.x" AND the desire_name: Start a new table
        if first_cell.startswith("6.") and desired_name in first_cell:
            # Found start of new desired table
            # print(f"Found start of new desired table:{first_cell}") 
            
            if current_table is None:
                current_table = table.df.copy()
            
            # print(f"Found start of new desired table:")   
            # display(current_table)

        
        #if the table doesnt match the desire_format AND there is a current_table: concat this table into the current table:         
        elif current_table is not None and not table.df.iloc[0][1].startswith("6.") and not table.df.iloc[0][0].startswith("6.") :  
            # Continuation of previous desired table
            # print(f"Found continuation of previous desired table: {table.df.iloc[0][1]}")
            # print(f"test: {table.df.iloc[0][1]}")
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
            # print(f"Skipping a none-desired table: {first_cell}")
            if current_table is not None:
                # Save the current table
                # print(f"Saving table: {current_table}")
                desired_tables.append(current_table)
                #Reset, mark end of the desired table
                current_table = None
                       
    # Check if the last table in the list was a continuation of a desired table
    if current_table is not None:
        # print(f"Saving table: {current_table}")
        desired_tables.append(current_table)
        row_before_process = current_table.shape[0]
    return desired_tables
def process_tables(tables):  

    processed_tables = []
    
    for i, table in enumerate(tables):
        # print(f"\nProcessing table {i+1}...")
        
        # Reset the index
        table = table.reset_index(drop=True)
        print(f"Table before processing:")
        display(table)
        
        # Fix rows that are split across pages
        print(f"Fixing rows that are split across pages...")
        for i in range(len(table)-1):
            # Check if the row should be joined with the preceding row
            # print(f"index {i}: {table.iloc[i, 0]}")
            if table.iloc[i, 0].startswith('UL_MOD_RB:') :
                print(f"Found a row to join: index {i}: {table.iloc[i, 0]}")
                # Join the rows
                table.iloc[i-1, 0] += '\n' + table.iloc[i, 0]
                # # Drop the current row
                # table.drop(table.index[i], inplace=True)
        # print(f"Table after joining rows:{table}")
    
        # Split the first cell of the first row and use it as column headers
        first_row = table.iloc[0, 0]
        headers = first_row.split('\n')  
        # print(f"First row: {first_row}")
        print(f"Headers: {headers}")
        #print name of columns in table:
        # print(f"Table columns: {table.columns.values}")
      
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

        # print(f"Table before drop columns:")
        # display(table)
        # Drop rows with NaN values in Testname
        table = table.dropna(subset=['Testname'], how='all')


        # Reset the index
        table.reset_index(drop=True, inplace=True)
        # print(f"Table after processing:{table}")

        #Append processed table to processed_tables:
        processed_tables.append(table)
        
        
    return processed_tables


def select_file():
    # Add a GUI file picker
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    return file_path



def concatenate_tables(clean_tables, target_table, pdf_file):
    # Concatenate all the dataframes in the list into a single dataframe
    # Extract name without extension
    file_name = os.path.splitext(os.path.basename(pdf_file))[0]

    if not clean_tables:
        print("No tables extracted")
        return pd.DataFrame()
    else:
        all_tables = pd.concat(clean_tables, ignore_index=True)
        display(all_tables.head(10)) # Display the first few rows of the resulting dataframe
        # Save to Excel using file name
        excel_file = f"{target_table}_{file_name}.xlsx"
        all_tables.to_excel(excel_file, index=False)

        # Auto open folder:
        folder_path = os.path.abspath(os.path.dirname(excel_file))
        if platform.system() == 'Windows':
            os.startfile(folder_path)
        elif platform.system() == 'Darwin': 
            subprocess.Popen(['open', folder_path])
        else:
            subprocess.Popen(['xdg-open', folder_path])

        return all_tables

# "LTE_3GPP_v15_r8_FDD_FORD_All_TEMPS_TCU2_5_ROW_012023_5GSIM_AT_2023-06-28_16-27-57_188.pdf"
#  "25C_DATA_Extract(1temperatureOnly).pdf"
# "300_pages_extract.pdf"
"30pages_6.6.2.3_Adjacent_Channel.pdf"



#Run through all pages, get table, concatentate, clean --> clean dataframes:
import multiprocessing
from multiprocessing import Pool
from functools import partial

def process_page_range(pdf_file, target_table, page_range):
    print(f"\nProcessing: {page_range}")

    start_time = time.time()
    # Read all tables from pdf
    long_tables = camelot.read_pdf(pdf_file, pages=page_range, backend="poppler")
    end_time = time.time()
    print(f"Time taken to load table: {end_time - start_time:.2f} seconds")

    start_time = time.time()
    desire_tables = find_target_table(long_tables, target_table)
    end_time = time.time()
    print(f"Time taken to find target table: {end_time - start_time:.2f} seconds")

    start_time = time.time()
    print(f"Processing tables...")
    processed_table = process_tables(desire_tables)
    end_time = time.time()
    print(f"Time taken to process tables: {end_time - start_time:.2f} seconds")
    # If it's a list of DataFrames, concatenate them
    if all(isinstance(table, pd.DataFrame) for table in processed_table):
        processed_table_df = pd.concat(processed_table, ignore_index=True)
    else:
        processed_table_df = pd.DataFrame(processed_table)

    return processed_table_df

def run(pdf_file, target_table):
    start_time = time.time()
    # Find target table pages location:
    pages = find_table_location(pdf_file, target_table)

    # Format page numbers for camelot
    page_ranges = convert_to_ranges(pages)
    #write to file:
    with open("page_ranges_{pdf_file}.txt", "w") as f:
        for page in page_ranges:
            f.write(f"{page}\n")
    
    # page_ranges = ['75-91', '426-446', '910-930', '1347-1367', '1812-1826', '2142-2158', '2434-2448', '2764-2780', '3073-3093', '3444-3460', '3729-3745', '4079-4095']
    print(page_ranges)
    print(f"Found {len(page_ranges)} tables")
    # Create a pool of processes
    with Pool(multiprocessing.cpu_count()) as p:
        func = partial(process_page_range, pdf_file, target_table)
        clean_tables = p.map(func, page_ranges)

    concatenate_tables(clean_tables, target_table, pdf_file)
    end_time = time.time()
    print(f"Run time: {end_time - start_time:.2f} seconds")
    
#options
maximum_table = "6.2.2 Maximum Output Power" 
adjacent_table = "6.6.2.3 Adjacent Channel Leakage Power Ratio" 
long_file = "22k.pdf"
shortFile = "6.6.2.3 500 pages.pdf"

#Set up
target_table = adjacent_table
pdf_file = long_file

# pdf_file = select_file()
if __name__ == '__main__':
    multiprocessing.freeze_support()  # Only required if you plan to build an executable. Can be removed otherwise.
    target_table = "6.6.2.3 Adjacent Channel Leakage Power Ratio" 
    pdf_file = "4436pages.pdf"

    run(pdf_file, target_table)
