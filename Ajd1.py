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
import logging
logging.basicConfig(level=logging.INFO)
import multiprocessing
from multiprocessing import Pool, cpu_count
from functools import partial

class PDFTableExtractor:

    def __init__(self, pdf_file, target_table):
        self.pdf_file = pdf_file
        self.target_table = target_table
        
        
    @staticmethod
    def select_file(self):
        # Add a GUI file picker
        root = tk.Tk()
        root.withdraw()
        file_path = filedialog.askopenfilename()
        return file_path

    def find_table_location(self):
        print(f"Searching for table: {self.target_table}")

        reader = PdfReader(self.pdf_file)

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
            if self.target_table in text:  
                target_index = text.index(self.target_table)
                #Check, find start of target table:
                if "Limit Low" in text[target_index:target_index+56]:
                    print(f"Start new target table on page {page_num+1}")
                    cont_table = True
                    pages_with_table.append(page_num+1)
                else:
                    print(f"Found a false positive on page {page_num+1}")
                    
            #Continuing table end if Cont_table is true AND:
            elif cont_table == True:
                
                #CHECK IF PAGE IS EMPTY
                # The header is the first two lines of the page
                header = '\n'.join(text.split('\n')[:2])
                # The footer is the last two lines of the page
                footer = '\n'.join(text.split('\n')[-2:])
                # Check if the page is empty
                if not text.strip() or not text.strip(header).strip(footer):
                    page_is_empty = True
                    print(f"Empty page: {page_num}")
      
                #1: found the 6.x.x.x pattern
                #2: found the Resource Block pattern
                #3: empty page
                if re.search(r'\d+\.\d+\.\d+', text) or "Resource Block" in text or page_is_empty:
                    if page_num == len(reader.pages)-1:
                        print(f"End of table on page {page_num}")
                        cont_table = False
                        pages_with_table.append(page_num)
                    else:
                        print(f"End of table on page {page_num}")
                        cont_table = False
                        pages_with_table.append(page_num)      
                              
            #If none of the above, continue searching
            else:
                pass
                
        return pages_with_table
    
    def clean_table(self, table):
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

    def get_page_ranges(self):
        # Create filename to cache page ranges
        ranges_text_file = f"{self.pdf_file}_{self.target_table}.txt"  

        if os.path.exists(ranges_text_file):

            print(f"Loading page ranges from {ranges_text_file}")
            
            with open(ranges_text_file, "r") as f:
                page_ranges = eval(f.read())

        else:

            print(f"Finding page ranges for {self.target_table}")
            
            pages = self.find_table_location()
            page_ranges = self.convert_to_ranges(pages)
            
            print(f"Saving page ranges to {ranges_text_file}")
            
            with open(ranges_text_file, "w") as f:  
                f.write(str(page_ranges))

        return page_ranges
    
    def estimate_total_time(self, page_ranges, load_times, process_times):

        num_ranges = len(page_ranges)
        
        avg_load_time = sum(load_times) / len(load_times)
        avg_process_time = sum(process_times) / len(process_times)
        
        est_total = num_ranges * (avg_load_time + avg_process_time)
        
        print(f"Average load time: {avg_load_time:.2f} secs")
        print(f"Average process time: {avg_process_time:.2f} secs")
        print(f"Estimated total time: {est_total/60:.2f} mins")
    
    def convert_to_ranges(self, numbers):
        ranges = []
        for i in range(0, len(numbers), 2):
            if i < len(numbers) - 1:
                ranges.append(f"{numbers[i]}-{numbers[i+1]}")
            else:
                ranges.append(str(numbers[i]))

        return ranges
    
    def find_target_table(self, tables):
        
        # Initialize variables
        continuous_table = None
        desired_tables = []
        rows_before_process = 0
        special_case = False
        
        # Loop through each table in the list of tables
        for table in tables:
            print("\n")
            # display(table.df.head(3))
            first_row = table.df.iloc[0]
            all_text_first_row = " ".join([str(cell) for cell in first_row]).strip()
            print(f"All text in first row: {all_text_first_row}")
            # Print the first two columns of the current table for debugging purposes
            # print(f"Colum 1: {col1}")
            # print(f"Colum 2: {col2}")
            pd.set_option('display.max_colwidth', 1000)
            display(table.df.head(3))
            
            # Check if the first cell of the current table is the start of the target table
            if all_text_first_row.startswith("6.") and self.target_table in all_text_first_row:
                print('Found start of target table')
                   
                pd.set_option('display.max_colwidth', 1000)
                display(table.df.head(3))
                # If the current table is the first table in the target table, copy it to current_table
                if continuous_table is None:
                    continuous_table = table.df.copy()
                    rows_before_process = continuous_table.shape[0]-2
                    
                    # Special case 1: If the current table has only one column
                    if continuous_table.shape[1] == 1:
                        special_case = True
                    # Special case 2: if the table has 7 columns, drop index 0 and 4 as they are extra
                    elif continuous_table.shape[1] == 7:
                        print("Special case 7 columns. Dropped empty columns 0 and 4")
                        continuous_table.drop([0], axis=1, inplace=True)
                        #Reset the column names
                        continuous_table.columns = range(continuous_table.shape[1])
                        display(continuous_table.head(3))
                    # elif continuous_table.shape[1] == 6:
                    #     continuous_table.drop([3], axis=1, inplace=True)
                    #     #Reset the column names
                    #     continuous_table.columns = range(continuous_table.shape[1])               
                    #     display(continuous_table.head(3))        
            # If the current table is not None and the first cell of the current table is not the start of the target table,
            # add the current table to the end of the previous table
            elif continuous_table is not None and not all_text_first_row.startswith("6."): 
                

                # Print debug information
                print(f"Found continuing table")
                print(f"Shape of table.df before cleaning: {table.df.shape}")
                print("Table before cleaning:") 
                display(table.df.head(3))                                  
                table.df = self.clean_table(table.df)
                print("Table after cleaning:")

                

                # Concatenate the current table and the new table
                continuous_table = pd.concat([continuous_table, table.df])
                rows_before_process += table.df.shape[0]
                
                # Handle special case if necessary
                if special_case:
                    continuous_table = self.handle_special_case(continuous_table)
                    special_case = False
                        
            # If the current table is None or the first cell of the current table is the start of a new table,
            # add the current_table to the list of desired_tables and set current_table to None
            else:
                if continuous_table is not None:
                    desired_tables.append(continuous_table)
                    current_table = None
            
        # If there is a current_table remaining (i.e. the last table in the list was part of the target table),
        # add it to the list of desired_tables
        if continuous_table is not None:
            desired_tables.append(continuous_table)
                
        # Return a list of desired tables and the number of rows before processing
        return desired_tables, rows_before_process

    def process_tables(self, tables):
        processed_tables = []
        
        for table in tables:
            table = table.reset_index(drop=True)
            
            print(f"Table before processing:\n")
            display(table)
            
            # Fix rows that are split across pages: detect and join it to row above
            for i in range(len(table)-1):
                if table.iloc[i, 0].startswith('UL_MOD_RB:'):
                    table.iloc[i-1, 0] += '\n' + table.iloc[i, 0]

            # Define headers list with the first part being self.target_table
            headers = [self.target_table, 'Limit Low', 'Limit High', 'Measured', 'Unit', 'Status']
            print(f"Headers: {headers}")
            table.columns = headers
            
            #Get table name
            table_name =  self.target_table
            
            #Extract band info
            table = self.extract_band_info(table)
            
            # Extraxt Testname, ULCH, etc
            testname_pattern = r"(.*):@"
            
            table['Testname'] = table.iloc[:,0].str.extract(testname_pattern)
            table['ULCH'] = table.iloc[:,0].str.extract(r"ULCH: (\d+),")
            table['BW'] = table.iloc[:,0].str.extract(r"BW: ([\d\.]+ MHz)")
            table['MOD'] = table.iloc[:,0].str.extract(r"UL_MOD_RB: ([^,]+),")
            table['RD'] = table.iloc[:,0].str.extract(r"UL_MOD_RB: [^,]+, (.*)")
            
            # Split Unit column
            split_df = table['Unit'].str.split(expand=True)
            table.loc[split_df[0].notna(), 'Measured'] = split_df.loc[split_df[0].notna(), 0]
            table.loc[split_df[1].notna(), 'Unit'] = split_df.loc[split_df[1].notna(), 1]
            table.drop(table.columns[0], axis=1, inplace=True)
            
            table.insert(0, 'Table Name', table_name)
            table = table.iloc[1:]
            
            new_column_order = ['Table Name', 'Testname', 'Band', 'ULCH', 'BW', 'MOD', 'RD', 'Limit Low', 'Limit High', 'Measured', 'Unit', 'Status']
            table = table.reindex(columns=new_column_order)
            table = table.dropna(subset=['Testname'], how='all')
            table.reset_index(drop=True, inplace=True)

            processed_tables.append(table)

        return processed_tables
    
    def extract_band_info(self, table):
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
        return table
    def handle_special_case(self, table):
        for row_index in range(table.shape[0]):
            if "dB" in table.iloc[row_index, 0] or "Passed" in table.iloc[row_index, 0]:
                row_parts = table.iloc[row_index, 0].split('\n')
                table.iloc[row_index, 1] = row_parts[1] 
                table.iloc[row_index, 4] = row_parts[3]
                table.iloc[row_index, 5] = row_parts[4]
                table.iloc[row_index, 2] = '---'
                del row_parts[1:5]
                table.iloc[row_index, 0] = '\n'.join(row_parts)

        return table

    def concatenate_tables(self, tables):
        #Extract file name without extension
        file_name = os.path.splitext(os.path.basename(self.pdf_file))[0]

        if not tables:
            print("No tables extracted")
            return pd.DataFrame()
        else:
            all_tables = pd.concat(tables, ignore_index=True)
            table_name = self.target_table.split()[1]
            
            # Add 2 new column
            # Extract temperature 
            filename = os.path.basename(self.pdf_file) 
            temp = re.search(r'_TEMPHERE(\S+)\.', filename).group(1)
            all_tables['Temperature'] = temp
              # Extract PDF filename 
            pdf_name = os.path.basename(self.pdf_file)
            all_tables['PDF Name'] = pdf_name
            
            excel_file = f"{table_name}_{file_name}.xlsx"
            all_tables.to_excel(excel_file, index=False)

            folder_path = os.path.abspath(os.path.dirname(excel_file))
            if platform.system() == 'Windows':
                os.startfile(folder_path)
            elif platform.system() == 'Darwin':
                subprocess.Popen(['open', folder_path])
            else:
                subprocess.Popen(['xdg-open', folder_path])

            return all_tables
    
    def process_page_range(self, target_table, page_range):
        print(f"Page range: {page_range}")
        # Read all tables from pdf
        start_time = time.time()
        long_tables = camelot.read_pdf(self.pdf_file, pages=page_range, backend="poppler")
            # display_tables(long_tables)
        end_time = time.time()
        print(f"Time taken to load table: {end_time - start_time:.2f} seconds")
        
        # Find and concatenate target tables:
        desire_tables, rows_before_processing = self.find_target_table(target_table)
        
        
        # Process/clean tables:
        start_time = time.time()
        processed_table = self.process_tables(desire_tables)
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
    def run(self):
        load_times = []
        process_times = []

        # Get page ranges, loading from cache if exists
        page_ranges = self.get_page_ranges()

        # Print out number of tables found
        print(f"Found {len(page_ranges)} tables")
        # Print page ranges for debugging
        print(f"Page ranges: {page_ranges}")

        # Create a pool of worker processes
        with Pool(cpu_count()) as p:
            # Apply the function to each page range in parallel
            func = partial(process_page_range, self)
            clean_tables = p.map(func, page_ranges)

        # Concatenate all clean tables into final DataFrame
        final_df = self.concatenate_tables(clean_tables)

        return final_df

    
# Add this function outside the class
def process_page_range(extractor, page_range):
    return extractor.process_page_range(page_range)

 
adjacent_table = "6.6.2.3 Adjacent Channel Leakage Power Ratio" 
target_table = adjacent_table


# Usage:
# pdf_file = select_file()
if __name__ == '__main__':
    multiprocessing.freeze_support()  # Only required if you plan to build an executable. Can be removed otherwise.

    #User select multiple files:
    root = tk.Tk()
    root.withdraw()
    filepaths = filedialog.askopenfilenames()

    if len(filepaths) == 1:
        # Single file selected
        extractor = PDFTableExtractor(filepaths[0], target_table)
        final_df = extractor.run()

    else:
        # Multiple files selected
        all_tables = []
        for filepath in filepaths:
            extractor = PDFTableExtractor(filepath, target_table)
            df = extractor.run()  # Changed variable name to `df`
            all_tables.append(df)  # Appending `df` instead of `tables`
        
        # Concatenate results:
        final_df = pd.concat(all_tables, ignore_index=True)

    # Save to Excel
    excel_file = "Final_Table.xlsx"
    final_df.to_excel(excel_file, index=False)  

    # Open file
    if platform.system() == 'Windows':
        os.startfile(excel_file)
    elif platform.system() == 'Darwin':
        subprocess.Popen(['open', excel_file])
    else:
        subprocess.Popen(['xdg-open', excel_file])
