"""
Purpose:
- Execute `RetailCategories <year>` stored procedure.
- Export the yearly result to the corresponding worksheet.
- Apply minimal formatting for readability in reports.
"""

import pyodbc
import pandas as pd
import os
import xlwings as xw
from datetime import date

# Read-only SQL user used for reporting jobs.
password = r'r3adp%78onlygmg'

# Build ODBC connection details for SQL Server.
conn_str = f"""DRIVER={{SQL Server}};
SERVER=MG-SERVER002;
DATABASE=soft1;
UID=jobview;
PWD={password};"""
# Use current year so each run writes to the yearly sheet.
year = str(date.today().year)
stored_procedure = "RetailCategories " + year
excel_path = r"\\MG-SERVER002\ShareData\reports\Πρότυπα Αρχεία\RetailCategories.xlsx"
sheet_name = year

# Entire ETL is wrapped in try/except to keep scheduler output clean.
try:
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        # Execute yearly stored procedure and load output to DataFrame.
        cursor.execute(f"EXEC {stored_procedure}")
        # Read metadata and rows from the resultset.
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        # Convert SQL rows to Pandas for convenient export.
        df = pd.DataFrame.from_records(rows, columns=columns)
        print(f"Rows fetched: {len(df)}")
        # Ensure destination path exists.
        os.makedirs(os.path.dirname(excel_path), exist_ok=True)

        #if sheet_name in [sheet.name for sheet in wb.sheets]:

        # Use hidden Excel app to avoid UI popups on server runs.
        with xw.App(visible=False) as app:
            # Open existing workbook (or create new) and select the target year sheet.
            if os.path.exists(excel_path):
                wb = xw.Book(excel_path, password="MG1234")
                # Reuse yearly sheet if already present, otherwise add it.
                if sheet_name in [sheet.name for sheet in wb.sheets]:
                    sheet = wb.sheets[sheet_name]
                    # Clear previous content so new export replaces old data.
                    sheet.clear()
                else:
                    sheet = wb.sheets.add(sheet_name)
            else:
                # Create workbook when file does not yet exist.
                wb = xw.Book()
                sheet = wb.sheets[0]
                sheet.name = sheet_name
            # Write header row and all data rows.
            sheet.range('A1').value = [df.columns.tolist()] + df.values.tolist()

            # Bold header row to distinguish fields from data rows.
            header_last_col = sheet.range('A1').expand('right').last_cell.column
            sheet.range((1, 1), (1, header_last_col)).font.bold = True
            
            # Format numeric column used for percentages/values.
            sheet.range("I:I").number_format = "0.00"

            # Persist workbook updates.
            wb.save(excel_path)
        
        print(f"Excel exported to:\n{excel_path}")

except Exception as e:
    print("ERROR:")
    print(e)
    
#pyinstaller --onefile RetailCategories.py # \\MG-SERVER002\ShareData\reports\exe