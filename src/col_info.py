# Version 9/24/26
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
            self.DropVBAColInfoCols()

        self.filTbl = None
    """
    =========================================================================
    InitColInfoProcedure - called by ColumnInfo.__init__()
    =========================================================================
    """
    def ImportColInfoDf(self, files):
        """
        Import ColInfo.df from Excel file
        JDL 5/28/25; updated 9/18/26
        """
        self.df = pd.read_excel(files.pf_col_info, sheet_name='colinfo_')

    def RecodeColInfoFlagCols(self):
        """
        Recode ColInfo flag columns to boolean (from imported True/NaN)
        JDL 5/28/25; updated 9/24/26
        """
        # Loop over rows to avoid deprecation warning with .fillna (9/18/26)
        flag_cols = ['IsCalculated', 'IsIndex']
        for col in flag_cols:
            vals = []
            for idx in self.df.index:
                val = self.df.at[idx, col]
                vals.append(bool(val) if pd.notna(val) else False)
            self.df[col] = vals

    def DropVBAColInfoCols(self):
        """
        Drop VBA-specific columns from ColInfo.df
        JDL 9/24/26
        """
        vba_cols = ['data_type_VBA', 'FillVals', 'FilterVals']
        for col in vba_cols:
            if col in self.df: self.df.drop(columns=col, inplace=True)

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
        JDL 5/28/25; updated 9/24/26
        """
        # Filter for variables with raw/import name and replacement name both defined
        fil = (~tbl.dfColInfo['VarNameRaw'].isna()) & (~tbl.dfColInfo['VarNameNorm'].isna())

        # Use dictionary to map old column names to new ones
        keys, vals = tbl.dfColInfo.loc[fil, 'VarNameRaw'], tbl.dfColInfo.loc[fil, 'VarNameNorm']
        tbl.df.rename(columns=dict(zip(keys, vals)), inplace=True)

    def SetImportedKeepCols(self, tbl):
        """
        Subset imported columns for tbl
        JDL 5/22/25; Updated 9/24/26
        """
        # Filter to (non-calculated) keep columns
        fil = (tbl.dfColInfo[tbl.name] > 0.) & (~tbl.dfColInfo['IsCalculated'])

        # Make a sorted list of col names and reset to those keep columns
        lst = tbl.dfColInfo.loc[fil].sort_values(tbl.name)['VarNameNorm'].tolist()
        tbl.df = tbl.df[lst]

    def SetTblDataTypes(self, tbl):
        """
        Set data types for tbl.df columns based on tbl.dfColInfo data_type_python column
        5/22/25; Updated 9/24/26
        """
        # Filter to (non-calculated) keep columns with data_type specified
        fil = (tbl.dfColInfo[tbl.name] > 0.) & (~tbl.dfColInfo['IsCalculated'])
        fil = fil & (~tbl.dfColInfo['data_type_python'].isna()) & (~tbl.dfColInfo['VarNameNorm'].isna())

        df_types = tbl.dfColInfo.loc[fil, ['VarNameNorm', 'data_type_python']]

        # Convert column data to specified type
        for col, data_type in zip(df_types['VarNameNorm'], df_types['data_type_python']):
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
        JDL 9/24/26
        """
        # Filter for columnns with and index order specified
        fil = tbl.dfColInfo['IsIndex']

        # Get the filtered list based on the tbl nam column and sort by idx_order
        lst_index_cols = tbl.dfColInfo.loc[fil].sort_values(tbl.name)['VarNameNorm'].tolist()
        tbl.idx = lst_index_cols