# Version 6/6/25
import pandas as pd
import numpy as np

class ParseColMajorTbl():
    """
    Parse data in columns with categories in column 0 rows
    (Format is very similar to InterleavedColBlocksTbl, but with a single col
    per block and no block name; could modify InterleavedColBlocksTbl to detect
    start flag instead of fixed idx_start)
    JDL 6/6/25
    """
    def __init__(self, tbl):
        # Raw DataFrame from input table
        self.df_raw = tbl.df_raw

        # Output DataFrame
        self.df = pd.DataFrame()

        # Data start and end flags and their column indices
        self.flag_start = tbl.dParseParams['flag_start_bound']
        self.flag_end = tbl.dParseParams['flag_end_bound']
        self.idx_start_flag_col = tbl.dParseParams['icol_start_flag']
        self.idx_end_flag_col = tbl.dParseParams['icol_end_flag']

        # Row offsets for header and data rows from the start and end flags
        self.header_row_offset = tbl.dParseParams['nrows_header_offset_from_flag']
        self.data_start_row_offset = tbl.dParseParams['nrows_data_offset_from_flag']
        self.data_end_row_offset = tbl.dParseParams['nrows_data_end_offset_from_flag']

        # Data boundary indices and list of categories
        self.idx_header_row = None
        self.idx_data_start = None
        self.idx_data_end = None
        self.lstCategories = None

        # Iteration variables
        self.idx_col_cur = None

    def ParseDfRawProcedure(self):
        """
        Procedure to parse blocks of columns in self.df_raw and set self.df
        JDL 6/6/25
        """
        self.FindDataBoundaries()
        self.SetDfCategories()
        self.TransferAllCols()
    
    def FindDataBoundaries(self):
        """
        Set data boundary indices for parsing using parse params and flag columns.
        JDL 6/6/25
        """
        # Find the row index where flag_start appears in the specified start column
        idx_start_flag = self.df_raw[self.df_raw.iloc[:, self.idx_start_flag_col] == self.flag_start].index[0]

        # Find the row index where flag_end appears in the specified end column, after start
        end_flag_rows = self.df_raw[self.df_raw.iloc[:, self.idx_end_flag_col] == self.flag_end].index
        end_flag_rows = end_flag_rows[end_flag_rows > idx_start_flag]
        idx_end_flag = end_flag_rows[0]

        # Set header, data start, and data end indices using row offsets from start flag
        self.idx_header_row = idx_start_flag + self.header_row_offset
        self.idx_data_start = idx_start_flag + self.data_start_row_offset
        self.idx_data_end = idx_end_flag + self.data_end_row_offset
        
    def SetDfCategories(self):
        """
        Set list of categories from the first column between data start and end
        (limited by hard-coded column 0; should refactor use use dParseParams to set attribute)
        JDL 6/6/25
        """
        self.lstCategories = self.df_raw.iloc[self.idx_data_start:self.idx_data_end+1, 0].tolist()

    def TransferAllCols(self):
        """
        Iterate over data columns and transfer their data to self.df
        JDL 6/6/25
        """
        # Loop over cols; Stop if blank header cell (can happen if df_raw has trailing blank cols)
        for self.idx_col_cur in range(1, self.df_raw.shape[1]):
            if pd.isna(self.df_raw.iloc[self.idx_header_row, self.idx_col_cur]): break

            # Read and write the column data
            self.ReadWriteColData()

    def ReadWriteColData(self):
        """
        Add one date column's data to self.df using self.idx_col_cur.
        """
        # Read the date from the header row for this column
        header_val = self.df_raw.iloc[self.idx_header_row, self.idx_col_cur]

        # Read the values for this column (orders)
        row_slice = slice(self.idx_data_start, self.idx_data_end + 1)
        n_orders = self.df_raw.iloc[row_slice, self.idx_col_cur].tolist()

        # Build a DataFrame for this column's data
        df_col = pd.DataFrame({'col_header': [header_val] * len(self.lstCategories),
            'category': self.lstCategories, 'value': n_orders})

        # Append column's data to self.df
        self.df = pd.concat([self.df, df_col], ignore_index=True)

"""
================================================================================
InterleavedColBlocksTbl Class - Data in interleaved, repeating column blocks
================================================================================
"""
class InterleavedColBlocksTbl():
    """
    Table contains initial metadata columns (row major) followed by interleaved
    repeating blocks of columns containing row major data. Block ID variable is
    in Row 1 first column of each block. Repeating variable names in Row 2
    """
    def __init__(self, tbl):

        #Raw DataFrame and column list parsed from raw data
        self.df_raw = tbl.df_raw

        # Start index to allow for initial, blank/unused columns
        self.idx_start = 0
        if 'idx_start' in tbl.dParseParams:
            self.idx_start = tbl.dParseParams['idx_start']

        # List of metadata columns preceding interleaved blocks
        self.n_cols_metadata = tbl.dParseParams['n_cols_metadata']

        # n columns per block
        self.n_cols_block = tbl.dParseParams['n_cols_block']

        self.df = pd.DataFrame()

        # Iteration variables
        self.idx_col_block_cur = None
        self.block_name_cur = None
        self.idx_col_cur = None

    def ParseDfRawProcedure(self):
        """
        Procedure to parse interleaved blocks of columns
        JDL 3/17/25; Name updated 5/30/25
        """
        self.SetDfMetadata()
        self.DeleteTrailingRows()
        self.TransferAllBlocks()
    
    def SetDfMetadata(self):
        """
        Set .df_metadata as a subset of .df_raw 
        JDL 
        """
        # Subset the metadata columns starting at idx_start
        col_last = self.idx_start + self.n_cols_metadata
        self.df_metadata = self.df_raw.iloc[2:, self.idx_start:col_last]
        self.df_metadata = self.df_metadata.reset_index(drop=True)
        self.df_metadata.columns = self.df_raw.iloc[1, self.idx_start:col_last]
        self.df_metadata.columns.name = None

    def DeleteTrailingRows(self):
        """
        Delete trailing rows with blank metadata
        JDL 3/17/25
        """
        # Find the index of last non-null metadata row
        idx_last = self.df_metadata.apply(lambda row: \
            not row.isnull().all(), axis=1).cumsum().idxmax()

        # Delete trailing rows from .df_metadata and corresponding .df_raw rows
        self.df_metadata = self.df_metadata.iloc[:idx_last + 1]
        self.df_raw = self.df_raw.iloc[:idx_last + 3]

    def TransferAllBlocks(self):
        """
        Transfer all blocks of columns to .df
        JDL 3/17/25
        """
        # Initialize .idx_col_cur
        self.idx_col_cur = self.idx_start + self.n_cols_metadata

        # Iteratively read blocks until end of columns
        while self.idx_col_cur < len(self.df_raw.columns) and \
            not pd.isna(self.df_raw.loc[0, self.idx_col_cur]):
            self.ReadWriteBlock()
            self.idx_col_cur += 1

    def ReadWriteBlock(self):
        """
        Read and write a block of columns to .df
        JDL 3/17/25
        """
        # Read the block name
        self.idx_col_block_cur = self.idx_col_cur
        self.block_name_cur = self.df_raw.loc[0, self.idx_col_block_cur]

        # Iteratively call ReadWriteColData for each column in the block
        for i in range(self.n_cols_block):
            self.idx_col_cur = self.idx_col_block_cur + i
            self.ReadWriteColData()

    def ReadWriteColData(self):
        """
        Transfer one column's data to .df by reading from a column block
        JDL 3/17/25
        """
        # Append .df_metadata to .df and write block name to new rows
        self.df = pd.concat([self.df, self.df_metadata], ignore_index=True)
        n_rows_new = len(self.df_metadata)
        self.df.loc[self.df.index[-n_rows_new:], 'block_name'] = self.block_name_cur

        # Write the variable name (.df_raw row index 1)
        var_name = self.df_raw.iloc[1, self.idx_col_cur]
        self.df.loc[self.df.index[-n_rows_new:], 'var_name'] = var_name

        # write values (as array to avoid index conflict between values and .df
        values = self.df_raw.iloc[2:, self.idx_col_cur].reset_index(drop=True)
        self.df.loc[self.df.index[-n_rows_new:], 'values'] = values.values
"""
================================================================================
RowMajorTbl Class - parsing files containing multiple row major blocks
================================================================================
"""
class RowMajorTbl():
    """
    Parse Row Major Table embedded in tbl.df_raw
    JDL 3/4/24; Modified 5/30/25
    """
    def __init__(self, tbl):
        
        #Raw DataFrame
        self.df_raw = tbl.df_raw

        #List of df indices for rows where flag_start_bound is found
        self.start_bound_indices = []

        #Table whose df is to be populated by parsing
        self.tbl = tbl

        #List of block IDs to extract (or individual tuple converted to list)
        self.lst_block_ids = tbl.dParseParams.get('block_id_vars', [])
        if isinstance(self.lst_block_ids, tuple): self.lst_block_ids = [self.lst_block_ids]

        # Output DataFrame
        self.df = pd.DataFrame()

        #Start, header, end, first data row indices for current block in loop
        self.idx_start_current = None
        self.idx_header_row = None
        self.idx_end_bound = None
        self.idx_start_data = None

        #Current block's columns and parsed data
        self.cols_df_block = []
        self.df_block = pd.DataFrame()
    """
    ================================================================================
    """
    def ParseDfRawProcedure(self):
        """
        Procedure to iteratively parse row major blocks into self.df
        JDL 9/26/24; Refactored 5/30/25
        """
        # Append blank row at end of .df_raw (to ensure find last <blank> flag)
        self.AddTrailingBlankRow()

        #Create list of row indices with start bound flag
        self.SetStartBoundIndices()

        #Iteratively read blocks 
        for i in self.start_bound_indices:
            self.idx_start_current = i
            self.ParseBlockProcedure()

        self.df = self.df.reset_index(drop=True)

    def AddTrailingBlankRow(self):
        """
        Add a trailing blank row to self.df_raw (to ensure last <blank> flag to
        terminate last block)
        JDL 9/26/24
        """
        blank_row = pd.Series([np.nan] * len(self.df_raw.columns), index=self.df_raw.columns)
        self.df_raw = pd.concat([self.df_raw, pd.DataFrame([blank_row])], ignore_index=True)

    def SetStartBoundIndices(self):
        """
        Populate list of row indices whereflag_start_bound is found
        JDL 9/25/24
        """
        flag= self.tbl.dParseParams['flag_start_bound']
        icol = self.tbl.dParseParams['icol_start_bound']

        fil = self.df_raw.iloc[:, icol] == flag
        self.start_bound_indices = self.df_raw[fil].index.tolist()

    def ParseBlockProcedure(self):
        """
        Parse the table and set self.df resulting DataFrame
        JDL 9/25/24; Modified 5/30/25
        """
        self.FindFlagEndBound()
        self.ReadHeader()
        self.SubsetDataRows()
        
        #Update .df_block with block_id vals(self arg is current RowMajorTbl instance)
        if 'block_id_vars' in self.tbl.dParseParams: 
            self.df_block = RowMajorBlockID(self).ExtractBlockIDs

        #Concatenate into tbl.df and re-initialize df_block
        self.df = pd.concat([self.df, self.df_block], axis=0)
        self.df_block = pd.DataFrame()

    def FindFlagEndBound(self):
        """
        Find index of flag_end_bound
        JDL 3/4/24; modified 9/26/24
        """
        flag = self.tbl.dParseParams['flag_end_bound']
        icol = self.tbl.dParseParams['icol_end_bound']
        ioffset = self.tbl.dParseParams['idata_rowoffset_from_flag']

        #Start the search at the first data row based on data offset from flag
        i = self.idx_start_current + ioffset

        # search for specifie flag string/<blank> below row i
        if flag == '<blank>':
            self.idx_end_bound = self.df_raw.iloc[i:, icol].isnull().idxmax()
        else:
            self.idx_end_bound = self.df_raw.iloc[i:, icol].eq(flag).idxmax()

    def ReadHeader(self):
        """
        Read header based on iheader_rowoffset_from_flag.
        JDL 3/4/24; modified 9/26/24
        """
        # Calculate the header row index
        iheader_offset = self.tbl.dParseParams['iheader_rowoffset_from_flag']
        self.idx_header_row =  self.idx_start_current + iheader_offset

        # Set the column names (drop columns with blank header)
        self.cols_df_block = self.df_raw.iloc[self.idx_header_row].values

    def SubsetDataRows(self):
        """
        Subset raw data rows based on flags and idata_rowoffset_from_flag
        JDL 3/4/24; Modified 4/23/25
        """
        # Calculate the start index for the data
        self.idx_start_data = self.idx_start_current + \
            self.tbl.dParseParams['idata_rowoffset_from_flag']

        # Create df with block's data rows
        self.df_block = self.df_raw.iloc[self.idx_start_data:self.idx_end_bound]

        # Set column names
        self.df_block.columns = self.cols_df_block

        # Added 4/23 drop columns with null column name and all null values
        fil = ~self.df_block.columns.isnull() & self.df_block.notna().any()
        self.df_block = self.df_block.loc[:, fil]

"""
================================================================================
RowMajorBlockID Class - sub to RowMajorTbl for extracting block_id values
================================================================================
"""
class RowMajorBlockID:
    def __init__(self, parse):

        # Parse instance's tbl, df_raw and first data row index
        self.tbl = parse.tbl        
        self.df_raw = parse.df_raw
        self.idx_start_data = parse.idx_start_data

        #List of block IDs (orig from tbl.dParseParams['block_id_vars'])
        self.lst_block_ids = parse.lst_block_ids

        #Current (in progress) df_block
        self.df_block = parse.df_block

    @property
    def ExtractBlockIDs(self):
        """
        Property returns updated DataFrame and list of names.
        JDL 9/27/24
        """
        self.ExtractBlockIDsProcedure()
        return self.df_block

    def ExtractBlockIDsProcedure(self):
        """
        Procedure to extract block ID values from df_raw based on current block's
        idx_start_data and list of block_id tuples: (block_id_name, row_offset,
        col_index) where row_offset is offset from idx_start_data and col_index is 
        absolute column index where each block_id value is found.
        JDL 9/27/24; refactored 5/30/25
        """
        #Iterate over tuples and add columns to self.df_block
        for tup_block_id in self.lst_block_ids:
            self.SetBlockIDColValue(tup_block_id)

        #Reorder block_id columns to be first
        self.ReorderBlockIDCols()

    def ReorderBlockIDCols(self):
        """
        Reorder block_id columns to be first in self.df_block
        JDL 9/27/24
        """
        blockid_cols = [tup[0] for tup in self.lst_block_ids]
        others = [col for col in self.df_block.columns if col not in blockid_cols]
        self.df_block = self.df_block[blockid_cols + others]

    def SetBlockIDColValue(self, tup_block_id):
        """
        Set .df_block column from an individual block ID tuple
        JDL 9/27/24; Modified 4/22/25 for RowMajorTbl refactor
        """
        # tuple consists of (block_id_name, row_offset, col_index)
        name, row_offset, col_offset = tup_block_id[0], tup_block_id[1], tup_block_id[2]
        idx_row, idx_col = self.idx_start_data + row_offset, tup_block_id[2]

        # Add column with value to .df_block
        self.df_block[name] = self.df_raw.iloc[idx_row, idx_col]