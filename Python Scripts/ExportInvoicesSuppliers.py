"""
Purpose:
- Execute `ExportInvoicesSuppliers <year>` stored procedure.
- Export the yearly result to Excel.
- Expand comma-separated supplier values and normalize report formats.
"""

import pyodbc
import pandas as pd
import os
import xlwings as xw
from datetime import date

# Read-only SQL user used for scheduled reports.
password = r'r3adp%78onlygmg'

# ODBC SQL Server connection string.
conn_str = f"""DRIVER={{SQL Server}};
SERVER=MG-SERVER002;
DATABASE=soft1;
UID=jobview;
PWD={password};"""
# Year is passed to stored procedure and used as sheet name.
year = str(date.today().year)
stored_procedure = "ExportInvoicesSuppliers " + year
excel_path = r"\\MG-SERVER002\ShareData\reports\Πρότυπα Αρχεία\ExportInvoicesSuppliers.xlsx"
sheet_name = year

# Wrap full export pipeline to print friendly errors instead of crashing.
try:
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        # Execute yearly stored procedure and load output to DataFrame.
        cursor.execute(f"EXEC {stored_procedure}")
        # Extract column labels and data rows from cursor.
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        # Convert SQL output into DataFrame for easier export handling.
        df = pd.DataFrame.from_records(rows, columns=columns)
        print(f"Rows fetched: {len(df)}")
        # Ensure destination path exists.
        os.makedirs(os.path.dirname(excel_path), exist_ok=True)

        # Use hidden Excel instance for unattended execution.
        with xw.App(visible=False) as app:
            # Open existing workbook (or create new) and select the target year sheet.
            if os.path.exists(excel_path):
                wb = xw.Book(excel_path, password="MG1234")
                # Reuse or create yearly worksheet.
                if sheet_name in [sheet.name for sheet in wb.sheets]:
                    sheet = wb.sheets[sheet_name]
                    # Replace old content with new export.
                    sheet.clear()
                else:
                    sheet = wb.sheets.add(sheet_name)
            else:
                # Create workbook file and rename first sheet to current year.
                wb = xw.Book()
                sheet = wb.sheets[0]
                sheet.name = sheet_name
            # Write header row and all data rows.
            sheet.range('A1').value = [df.columns.tolist()] + df.values.tolist()

            # Make header visually distinct.
            header_last_col = sheet.range('A1').expand('right').last_cell.column
            sheet.range((1, 1), (1, header_last_col)).font.bold = True
            
            # Total rows include header row (row 1).
            total_rows = len(df) + 1

            # Expand comma-separated supplier values in column 33.
            for r in range(2, total_rows + 1):
                raw_value = sheet.range(r, 33).value

                # Keep empty cells untouched.
                if raw_value is None:
                    continue
                
                # Split supplier list by comma and trim spacing.
                tokens = [t.strip() for t in str(raw_value).split(",")]

                # Writing a list in xlwings spills across adjacent columns.
                sheet.range(r, 33).value = tokens
                
            # Force mixed-type report columns to general format.
            #sheet.range("Q:Q").number_format = "General"
            sheet.range("R:R").number_format = "General"
            sheet.range("S:S").number_format = "General"
            sheet.range("T:T").number_format = "General"
            sheet.range("U:U").number_format = "General"
            sheet.range("Z:Z").number_format = "General"
            sheet.range("AA:AA").number_format = "General"

            # Save final workbook.
            wb.save(excel_path)
        
        print(f"Excel exported to:\n{excel_path}")

except Exception as e:
    print("ERROR:")
    print(e)
    
#pyinstaller --onefile ExportInvoicesSuppliers.py # \\MG-SERVER002\ShareData\reports\exe