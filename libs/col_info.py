# Version 6/4/25
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime
"""
=============================================================================
Class ColumnInfo
=============================================================================
"""
class ColumnInfo:
    def __init__(self, files, IsInit=True, IsPrint=True):
        """
        Instance ColumnInfo (typically as cinfo); optionally initialize df
        JDL 5/22/25; updated 5/28/25
        """
        self.IsPrint = IsPrint

        # Import ColInfo from Excel file and set types of flag columns
        if IsInit:
            self.ImportColInfoDf(files)
            self.RecodeColInfoFlagCols()

        self.filTbl = None
    """
    =========================================================================
    InitColInfoProcedure - called by ColumnInfo.__init__()
    =========================================================================
    """
    def ImportColInfoDf(self, files):
        """
        Import ColInfo.df from Excel file
        JDL 5/28/25
        """
        self.df = pd.read_excel(files.pf_col_info, sheet_name='cols')

    def RecodeColInfoFlagCols(self):
        """
        Recode ColInfo flag columns to boolean (from imported True/NaN)
        JDL 5/28/25
        """
        flag_cols = ['IsCalculated']
        for col in flag_cols:
            self.df[col] = self.df[col].fillna(False).astype(bool)

    """
    =========================================================================
    CleanupImportedDataProcedure
    =========================================================================
    """
    def CleanupImportedDataProcedure(self, tbl):
        """
        Overall Procedure to subset/reorder imported columns and set data types 
        JDL 5/28/25
        """
        self.RenameColsRawData(tbl)
        self.SetImportedKeepCols(tbl)
        self.SetTblDataTypes(tbl)

    def RenameColsRawData(self, tbl):
        """
        Rename raw data columns post-import
        JDL 5/28/25
        """
        # Filter for variables with raw/import name and replacement name both defined
        fil = (~tbl.dfColInfo['cols_raw'].isna()) & (~tbl.dfColInfo['cols'].isna())

        # Use dictionary to map old column names to new ones
        keys, vals = tbl.dfColInfo.loc[fil, 'cols_raw'], tbl.dfColInfo.loc[fil, 'cols']
        tbl.df.rename(columns=dict(zip(keys, vals)), inplace=True)

    def SetImportedKeepCols(self, tbl):
        """
        Subset imported columns for tbl
        JDL 5/22/25; Updated 5/28/25
        """
        # Filter to (non-calculated) keep columns
        fil = self.SetFilterColInfoPopulated(tbl, ['cols_order', 'cols'], True)

        # Make a sorted list of cols names and reset to those keep columns
        lst = tbl.dfColInfo.loc[fil].sort_values('cols_order')['cols'].tolist()
        tbl.df = tbl.df[lst]

    def SetTblDataTypes(self, tbl):
        """
        Set data types for tbl.df columns based on self.df data_type column
        5/22/25; Updated 5/28/25
        """
        # Filter to (non-calculated) keep columns with data_type specified
        fil = self.SetFilterColInfoPopulated(tbl, ['data_type', 'cols'], True)

        #fil = (~tbl.dfColInfo['data_type'].isna()) & (~tbl.dfColInfo['cols'].isna())
        df_types = tbl.dfColInfo.loc[fil, ['cols', 'data_type']]

        # Convert column data to specified type
        for col, data_type in zip(df_types['cols'], df_types['data_type']):
            if data_type == 'dt.date':
                tbl.df[col] = pd.to_datetime(tbl.df[col]).dt.date
            elif data_type == 'datetime':
                tbl.df[col] = pd.to_datetime(tbl.df[col])

            # Use .astype directly on the data_type string
            else:
                tbl.df[col] = tbl.df[col].astype(data_type)

    """
    =========================================================================
    Other Methods
    =========================================================================
    """
    def SetTblIndexList(self, tbl):
        """
        Set tbl.idx to a list of index columns from tbl.dfColInfo
        JDL 6/4/25
        """
        # Filter for columnns with and index order specified
        fil = self.SetFilterColInfoPopulated(tbl, ['idx_order', 'cols'], True)

        # Get the filtered list and sort by idx_order
        lst_index_cols = tbl.dfColInfo.loc[fil].sort_values('idx_order')['cols'].tolist()
        tbl.idx = lst_index_cols


    """
    =========================================================================
    Utility Methods
    =========================================================================
    """
    def SetFilterColInfoPopulated(self, tbl, lst_cols, OmitIsCalculated=False):
        """
        Helper function to filter tbl.dfColInfo
        JDL 5/28/25
        """
        # Filter for specified columns non-null
        fil = tbl.dfColInfo[lst_cols].notna().all(axis=1)

        # Optionally omit calculated columns
        if OmitIsCalculated: fil = fil & (~tbl.dfColInfo['IsCalculated'])

        return fil


