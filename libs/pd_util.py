#Version 3/19/25 JDL
import pandas as pd
import numpy as np
import io
import contextlib

def dfExcelImport(sPF, sht=0, skiprows=None, IsDeleteBlankCols=False):
    """
    Import an Excel file optionally from specified sheet; delete extraneous columns
    Modified 12/5/23 to convert column names to strings in case they are integers
    """
    df = pd.read_excel(sPF, sheet_name=sht, skiprows=skiprows)

    #Delete Unnamed columns that result from Excel UsedRange bigger than detected data
    if IsDeleteBlankCols:
        lst_drop = [c for c in df.columns if str(c).startswith('Unnamed:')]
        df = df.drop(lst_drop, axis=1)
    return df

def Df_Roundup(df, n_decimals):
    """
    Roundup df values based on n_decimals precision
    JDL 2/20/23
    """
    df_scale = df * 10**n_decimals
    return np.ceil(df_scale) * 10**(-n_decimals)

def custom_info(df):
    """
    Custom function to display DataFrame information without dtypes and memory usage.
    """
    # Create a StringIO buffer
    buffer = io.StringIO()
    
    # Redirect stdout to the buffer
    with contextlib.redirect_stdout(buffer):
        df.info(memory_usage=True, show_counts=True)

    # Get the string from the buffer and convert to lines
    info_str = buffer.getvalue()
    info_lines = info_str.split("\n")

    # Print the lines except for the dtypes and memory usage
    for line in info_lines[:-3]:
        if line.startswith('Data columns'):
            pass
        else:
            print(line)