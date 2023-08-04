

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
import multiprocessing
from multiprocessing import Pool
from functools import partial
from tkinter import simpledialog, messagebox, Checkbutton, IntVar,Listbox, Scrollbar, END
from datetime import datetime

def read_pdf(file_path):
    # Read the PDF file using Camelot
    print(f"Start reading PDF file {file_path}...")
    tables = camelot.read_pdf(file_path, pages='all' )
    # clean_tables(tables)
    return tables

def pdf_to_text(pdf_file):
    reader = PdfReader(pdf_file)
    text = ''
    for page_num in range(len(reader.pages)):
        page = reader.pages[page_num]
        text += page.extract_text()
        print(f"Page num: {page_num+1}\nText: {text}")
    # Write the text to a file
    with open(f"{pdf_file}.txt", 'w') as file:
        file.write(text)

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
def display_preprocess_table(table):
    # Set the max column width to a high number (e.g., 1000) to display long contents
    pd.set_option('display.max_colwidth', 1000)
    # Display the table
    table.df.head()

def display_table(table):
    # Set the max column width to a high number (e.g., 1000) to display long contents
    pd.set_option('display.max_colwidth', 1000)
    # Display the table
    display(table.head())
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
    page_is_empty = False
    
    for page_num in range(len(reader.pages)):
        #print out page number after 1000 pages:
        if page_num % 1000 == 0:
            print(f"Searching page {page_num+1} out of {len(reader.pages)}")
        
        #Extract text from page:
        page = reader.pages[page_num]
        text = page.extract_text()
        
        
        #Found start of target table:
        if target_table in text:  
            target_index = text.index(target_table)
            #Check, find start of target table:
            if "Limit Low" in text[target_index:target_index+56]:
                print(f"Start new target table on page {page_num+1}")
                cont_table = True
                pages_with_table.append(page_num+1)
            else:
                print(f"Found a false positive on page {page_num+1}")
                
        #Continuing table end if Cont_table is true AND:
        elif cont_table == True:
            
            # #CHECK IF PAGE IS EMPTY
            # # The header is the first two lines of the page
            # header = '\n'.join(text.split('\n')[:2])
            # # The footer is the last two lines of the page
            # footer = '\n'.join(text.split('\n')[-2:])
            # # Check if the page is empty
            # if not text.strip() or not text.strip(header).strip(footer):
            #     page_is_empty = True
            #     print(f"Empty page: {page_num}")
    
            #1: found the 6.x.x.x pattern
            #2: found the Resource Block pattern
            #3: empty page: cant find a good solution yet
            #4: last pdf page
            if page_num == len(reader.pages)-1: #4: last pdf page
                print(f"End of table on page {page_num+1}, last pdf page.")
                cont_table = False
                pages_with_table.append(page_num+1)
                
            elif "Limit Low" in text  :  #1: found the 6.x.x.x pattern re.search(r'\d+\.\d+\.\d+', text)
                print(f"End of table on page {page_num+1}. (Limit Low in text)")
                # print(f"Text: {text}")
                cont_table = False
                pages_with_table.append(page_num+1)  
                    
            elif "Resource Block" in text:  #2: found the Resource Block pattern, get last page
                print(f"End of table on page {page_num}.  Resource Block")
                cont_table = False
                pages_with_table.append(page_num) 
                            
        #If none of the above, continue searching
        else:
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

def find_and_concat_target_table(tables, desired_name):
    
    current_table = None
    desired_tables = []
    rows_before_process = 0
    special_case = False

    #Run through table_list
    for table in tables:

        first_cell = table.df.iloc[0,0].split('\n')[0]
        # print(f"\nChecking table: {first_cell}")
        first_row = table.df.iloc[0]
        all_text_first_row = " ".join([str(cell) for cell in first_row]).strip()
        
        # print(f"\nChecking table: {all_text_first_row}")
        # display(table.df.head(3))
        
        #If the cell in first row, first column has the desire format "6.x.x.x" AND the desire_name: Start a new table
        if all_text_first_row.startswith("6.") and desired_name in all_text_first_row:
            # Found start of new desired table
            # print(f"\nFound start of new desired table:{all_text_first_row}") 
            # pd.set_option('display.max_colwidth', 1000)
            # display(table.df.head(3))
            if current_table is None:
                current_table = table.df.copy()
                rows_before_process = current_table.shape[0]-2
                
                # Check & handle special case:
                if table.df.shape[1] == 1:
                    print(f"Special case detected: {current_table.shape}")
                    # camelot.plot(table,kind='contour').show()
                    # camelot.plot(table,kind='joint').show()
                    special_case = True
                elif current_table.shape[1] == 7:
                        print("Special case 7 columns. Dropped empty columns 0")
                        current_table.drop([0], axis=1, inplace=True)
                        #Reset the column names
                        current_table.columns = range(current_table.shape[1])    
                    
                #     # Handle the special case
                #     current_table = handle_special_case(current_table)
                    
                # print(f"Found start of new desired table:")   
                # display(current_table.head())

        
        #if the table doesnt match the desire_format AND there is a current_table: concat this table into the current table:         
        elif current_table is not None and not all_text_first_row.startswith("6.") :  
            # Continuation of previous desired table
            # print(f"Found continuation of previous desired table: {table.df.iloc[0][1]}")
            # print(f"test: {table.df.iloc[0][1]}")
            # print(f"Found continuous table, before cleaning:")   
            # pd.set_option('display.max_colwidth', 1000)
            # display(table.df.head(3))
            #Clean table before concat:
            table.df = clean_table(table.df)
            # print(f"Continuous table after cleaning:")   
            # display(table.df)
        
            #CONCAT TABLE:
            current_table = pd.concat([current_table, table.df])
            rows_before_process += table.df.shape[0]
            # print(f"Table after concat:")
            # display(current_table)
            if special_case:
                # Handle the special case
                current_table = handle_special_case(current_table)
                special_case = False  # Reset the flag
            # print(f"Table after fix special case:")
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
        # print(f"Rows before processing: {rows_before_process}")
    return desired_tables, rows_before_process

def process_tables(tables, target_table):  

    processed_tables = []
    
    for i, table in enumerate(tables):
        # print(f"\nProcessing table {i+1}...")
      
        # Reset the index
        table = table.reset_index(drop=True)
        print(f"Table before processing:")
        pd.set_option('display.max_colwidth', 1000)
        display(table.head(10))
        
        # Fix rows that are split across pages
        print(f"Fixing rows that are split across pages...")
        for i in range(len(table)-1):
            # Check if the row should be joined with the preceding row
            # print(f"index {i}: {table.iloc[i, 0]}")
            if table.iloc[i, 0].startswith('UL_MOD_RB:') :
                # print(f"Found a row to join: index {i}: {table.iloc[i, 0]}")
                # Join the rows
                table.iloc[i-1, 0] += '\n' + table.iloc[i, 0]
                # # Drop the current row
                # table.drop(table.index[i], inplace=True)
        # print(f"Table after joining rows:{table}")
    
        # Split the first cell of the first row and use it as column headers
        headers = [target_table, 'Limit Low', 'Limit High', 'Measured', 'Unit', 'Status']
        # print(f"Headers: {headers}")
        table.columns = headers
        
        # Extract the table name from the first row
        table_name = target_table

        
        # Extract Band info from column 2
        # print(f"Extracting Band info...")
        line1 = table.iloc[0, 0]
        line2 = table.iloc[1, 0]
        if 'Band' in line2:
            band_info = line2.split(' ')
        elif 'Band' in line1:
            band_info = line1.split(' ')
        else:
            return table
        for word in band_info:
            if word.startswith('Band'):
                band_num = word[4:]
                table.insert(1, 'Band', band_num)
                break
         

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
        # display_table(table)
   
        # Split 'Measured' and 'Unit' columns 
        # Create a temporary DataFrame for the split results
        split_df = table['Unit'].str.split(expand=True)
        print(f"Split df head: {split_df.head()}")
        
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

        # Convert valid entries to float, leave invalid entries as NaN
        table['Band'] = pd.to_numeric(table['Band'], errors='coerce')
        table['ULCH'] = pd.to_numeric(table['ULCH'], errors='coerce')
        table['Limit Low'] = pd.to_numeric(table['Limit Low'], errors='coerce')
        table['Limit High'] = pd.to_numeric(table['Limit High'], errors='coerce')
        #If num, convert to float, else leave as is:
        table['Measured'] = table['Measured'].apply(lambda x: x if "Not Required" in str(x) else pd.to_numeric(x))

        # Reset the index
        table.reset_index(drop=True, inplace=True)
        # print(f"Table after processing:{table}")

        #Append processed table to processed_tables:
        processed_tables.append(table)
        
        
        
    return processed_tables

# Define a function that attempts to convert a series to numeric values
def convert_to_numeric(series):
    try:
        return pd.to_numeric(series)
    except ValueError:
        # If the series cannot be converted to numeric values, return it as is
        return series
    
def handle_special_case(current_table):
    """
    Handle special case where table at end of pdf: camelot parse weird format: missing info
    Table only has one column.
    Split the information in the cell into different columns.
    """
    # Loop through all the rows in the current table
    for row_index in range(current_table.shape[0]):

        # Check if the row contains "dB" or "Passed"
        if "dB" in current_table.iloc[row_index, 0] or "Passed" in current_table.iloc[row_index, 0]:

            # Split the row into different parts
            row_parts = current_table.iloc[row_index, 0].split('\n')

            # Assign each part to its corresponding column
            current_table.iloc[row_index, 1] = row_parts[1]
            current_table.iloc[row_index, 4] = row_parts[3]
            current_table.iloc[row_index, 5] = row_parts[4]
            current_table.iloc[row_index, 2] = '---'
            # Remove the parts that have been moved from the row_parts list
            del row_parts[1:5]

            # Join the remaining parts and update the original cell
            current_table.iloc[row_index, 0] = '\n'.join(row_parts)

    return current_table


def concatenate_tables(clean_tables, target_table, pdf_file):
    # Concatenate all the dataframes in the list into a single dataframe
    # Extract name without extension
    file_name = os.path.splitext(os.path.basename(pdf_file))[0]

    if not clean_tables:
        print("No tables extracted")
        return pd.DataFrame()
    else:
        all_tables = pd.concat(clean_tables, ignore_index=True)
        table_name = target_table.split()[1]
        display(all_tables.head(10)) # Display the first few rows of the resulting dataframe
        #Extract table name:
        table_name = target_table.split()[1] 
        
        
        # Add 2 new column
        # Extract temperature 
        filename = os.path.basename(pdf_file) 
        try:
            temp = re.search(r'_TEMPHERE(\S+)\.', filename).group(1)
            all_tables['Temperature'] = temp
        except AttributeError as e: 
            print(f"No temperature info found in filename: {filename}")
            
        # Extract PDF filename 
        pdf_name = os.path.basename(pdf_file)
        all_tables['PDF Name'] = pdf_name
        
        # Save to Excel using file name
        # excel_file = f"MUL_{table_name}_{file_name}.xlsx"
        # all_tables.to_excel(excel_file, index=False)

        # # Auto open folder:
        # folder_path = os.path.abspath(os.path.dirname(excel_file))
        # if platform.system() == 'Windows':
        #     os.startfile(folder_path)
        # elif platform.system() == 'Darwin': 
        #     subprocess.Popen(['open', folder_path])
        # else:
        #     subprocess.Popen(['xdg-open', folder_path])

        return all_tables

    
def process_page_range(pdf_file, target_table, page_range):
    print(f"Page range: {page_range}")
    # Read all tables from pdf
    start_time = time.time()
    long_tables = camelot.read_pdf(pdf_file, pages=page_range, backend="poppler")
        # display_tables(long_tables)
    end_time = time.time()
    print(f"Time taken to load table: {end_time - start_time:.2f} seconds")
    
    # Find and concatenate target tables:
    desire_tables, rows_before_processing = find_and_concat_target_table(long_tables, target_table)
    
    
    # Process/clean tables:
    start_time = time.time()
    processed_table = process_tables(desire_tables, target_table)
    end_time = time.time()
    print(f"Time taken to process tables: {end_time - start_time:.2f} seconds")
    
    # Check if processed_table is not empty and contains DataFrames
    if processed_table and all(isinstance(table, pd.DataFrame) for table in processed_table):
        processed_table_df = pd.concat(processed_table, ignore_index=True)
        #print number of tables concatenated:
        print(f"Concatenated {len(processed_table_df)} tables")
    else:
        print("No tables to concatenate.")
        processed_table_df = pd.DataFrame()  # Return an empty DataFrame

        
    return processed_table_df

def run(pdf_file, target_table):
    start_time = time.time()
    ranges_text_file = f"{pdf_file}_{target_table}.txt"
    
    # Check if page range text file exists:
    if os.path.exists(ranges_text_file):
        print(f"Check and load page ranges from text file: {ranges_text_file}")
        # Load page ranges from text file:
        with open(ranges_text_file, "r") as file:
            page_ranges = eval(file.read())
    else:
        # Find target table pages location:
        pages = find_table_location(pdf_file, target_table)

        # Format page numbers for camelot
        page_ranges = convert_to_ranges(pages)
        
        # Save page ranges to text file:
        with open(ranges_text_file, "w") as file:
            file.write(str(page_ranges))
        print(f"Saved page ranges to text file: {ranges_text_file}")
        
    # print(page_ranges)
    # print(f"Found {len(page_ranges)} tables")
    
    # Use a pool of worker processes
    with Pool(multiprocessing.cpu_count()) as p:
        # Use the partial function to make a new function that has the same parameters
        # as process_page_range but with pdf_file and target_table set as default parameters
        func = partial(process_page_range, pdf_file, target_table)
        # Run the new function in parallel on the list of page ranges
        clean_tables = p.map(func, page_ranges)
        
    final_df = concatenate_tables(clean_tables, target_table, pdf_file)
    
    end_time = time.time()
    print(f"\nRun time for table {target_table}: {end_time - start_time:.2f} seconds")
    print(f"Found {len(page_ranges)} tables")
    print(page_ranges)
    # display(clean_tables)
    
    return final_df

def select_files_and_tables():
    # Add a GUI file picker
    root = tk.Tk()
    root.withdraw()

    # User selects files
    file_paths = filedialog.askopenfilenames()

    # User selects target tables
    target_tables = [
        "6.2.2 Maximum Output Power",
        "6.6.2.3 Adjacent Channel Leakage Power Ratio",
        "6.2.3 Maximum Power Reduction"
    ]

    # Create Listbox for selecting tables
    root.deiconify()  # Show the root window
    root.title("Select target tables")
    listbox = Listbox(root, selectmode="multiple", exportselection=0)
    for table in target_tables:
        listbox.insert(END, table)
    listbox.pack()
    
    # Bring the window to the front and give it focus
    root.lift()
    root.focus_force()
    
    # Add scrollbar
    scrollbar = Scrollbar(root)
    scrollbar.pack(side="right", fill="y")
    listbox.config(yscrollcommand=scrollbar.set)
    scrollbar.config(command=listbox.yview)

    # Define selected_tables variable
    selected_tables = []

    # Function to be called when the 'Submit' button is clicked
    def submit_and_close():
        nonlocal selected_tables
        selected_indices = listbox.curselection()
        selected_tables = [listbox.get(i) for i in selected_indices]
        root.quit()
        root.destroy()

    # Add submit button
    submit_button = tk.Button(root, text='Submit', command=submit_and_close)
    submit_button.pack()

    root.mainloop()

    # User specifies output file name
    filename = simpledialog.askstring("Output Filename", "Enter output filename without extension. Or leave empty will auto naming.", initialvalue="")
    if filename == "" or filename is None:  # If user closes the dialog box or leave it empty, the filename is set to the first number in the table name and the current time
        # Get the first number in each selected table name
        table_numbers = [table.split(' ')[0] for table in selected_tables]
        # Join the table numbers with an underscore
        table_numbers_string = '_'.join(table_numbers)        # Get the current time
        current_time = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')  # This formats the current time as 'YYYYMMDDHHMMSS'
        filename = f"{table_numbers}_{current_time}"

    return file_paths, selected_tables, filename

def save_and_open_excel(df,filename):
    # Save to Excel
    excel_file = f"{filename}.xlsx"
    df.to_excel(excel_file, index=False)  
    print(f"Exported to Final Excel file: {excel_file}")
    
    # Open file
    if platform.system() == 'Windows':
        os.startfile(excel_file)
    elif platform.system() == 'Darwin':
        subprocess.Popen(['open', excel_file])
    else:
        subprocess.Popen(['xdg-open', excel_file])    


      

#options
# additionalMax_table = "6.2.4 Additional Maximum Power Reduction" 
# long_file = "/Users/huyknguyen/Desktop/paul-processing-tool/pdf/22k.pdf"
# med_file = "/Users/huyknguyen/Desktop/paul-processing-tool/pdf/4436pages.pdf"
# shortFile = "/Users/huyknguyen/Desktop/paul-processing-tool/pdf/6.6.2.3 500 pages.pdf"
# file_55c = "/Users/huyknguyen/Desktop/paul-processing-tool/pdf/LTE_3GPP_v15_r8_FDD_FORD_All_TEMPS_TCU2_5_ROW_012023_5GSIM_AT_2023-06-28_16-27-57_188_TEMPHERE55C.pdf"
# file_75c = "/Users/huyknguyen/Desktop/paul-processing-tool/pdf/LTE_3GPP_v15_r8_FDD_FORD_All_TEMPS_TCU2_5_ROW_012023_5GSIM_AT_2023-06-28_16-27-57_188_TEMPHERE75C.pdf"
# #Set up
# target_table = maximum_table
# new25c = "/Users/huyknguyen/Desktop/paul-processing-tool/pdf/LTE_3GPP_v15_r8_FDD_FORD_All_TEMPS_TCU2_5_ROW_012023_5GSIM_AT_2023-06-28_16-27-57_188_TEMP25C.pdf"
# pdf_file = file_55c



# # Define target tables
# maximum_table = "6.2.2 Maximum Output Power" 
# adjacent_table = "6.6.2.3 Adjacent Channel Leakage Power Ratio" 
# powerReduction_table = "6.2.3 Maximum Power Reduction"
# target_tables = [maximum_table, adjacent_table, powerReduction_table]


if __name__ == '__main__':
    start_time = time.time()
    
    multiprocessing.freeze_support()
    
    #User selections UI
    filepaths, target_tables, filename = select_files_and_tables()
    print(f"Selected files: {filepaths}")
    print(f"Selected tables: {target_tables}")
    print(f"Output filename: {filename}")
    
    # Process each target table
    all_dfs = []
    for target_table in target_tables:
        print(f"\nProcessing {target_table}...")
        if len(filepaths) == 1:
            # Single file selected
            final_df = run(filepaths[0], target_table)
        else:
            # Multiple files selected
            all_tables = []
            for filepath in filepaths:
                df = run(filepath, target_table)
                all_tables.append(df)  
            
            # Concatenate results:
            final_df = pd.concat(all_tables, ignore_index=True)
            
        all_dfs.append(final_df)
            
    # Concatenate all dataframes into one
    final_df = pd.concat(all_dfs, ignore_index=True)

    # Save and open the Excel file
    save_and_open_excel(final_df, filename)
    
    end_time = time.time()
    print(f"Total run time full program: {end_time - start_time:.2f} seconds")