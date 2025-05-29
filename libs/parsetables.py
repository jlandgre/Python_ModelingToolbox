# Version 5/29/25
import pandas as pd
import numpy as np
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

    def ParseInterleavedBlocksProcedure(self):
        """
        Procedure to parse interleaved blocks of columns
        JDL 3/17/25
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
RowMajorTbl Class - for parsing row major raw data single block
================================================================================
"""
class RowMajorTbl():
    """
    Description and Parsing Row Major Table initially embedded in tbl.df
    (imported with tbls.ImportInputs() or .ImportRawInputs() methods
    JDL 3/4/24; Modified 9/26/24
    """
    def __init__(self, tbl, df=None):
        
        # If df (raw data) is specified from tbl.lst_dfs iteration, use it
        if df is not None:
            self.df_raw = df
        else:
            self.df_raw = tbl.df_raw

        #List of df indices for rows where flag_start_bound is found
        self.start_bound_indices = []

        #Raw DataFrame and column list parsed from raw data
        #self.df_raw = tbl.df_raw

        #Table whose df is to be populated by parsing
        self.tbl = tbl
        self.lst_block_ids = []

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
    def ReadBlocksProcedure(self):
        """
        Procedure to iteratively parse row major blocks
        JDL 9/26/24
        """
        # Append blank row at end of .df_raw (to ensure find last <blank> flag)
        self.AddTrailingBlankRow()

        #Create list of row indices with start bound flag
        self.SetStartBoundIndices()

        #Iteratively read blocks 
        for i in self.start_bound_indices:
            self.idx_start_current = i
            self.ParseBlockProcedure()

        #Extract block_id values if specified (Note: self arg is RowMajorTbl instance)
        self.tbl.df, self.lst_block_ids = RowMajorBlockID(self).ExtractBlockIDs

        #set default index
        #self.SetDefaultIndex()
        self.tbl.df = self.tbl.df.reset_index(drop=True)

        #Optionally stack parsed data (if .dParams['is_stack_parsed_cols']
        #self.StackParsedCols()

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

    def SetDefaultIndex(self):
        """
        Set the table's default index
        JDL 3/4/24
        """
        self.tbl.df = self.tbl.df.set_index(self.tbl.idx_col_name)
    
    def StackParsedCols(self):
        """
        Optionally stack parsed columns from row major blocks
        JDL 9/25/24; Modified 4/23 remove from procedure/move to Apply ColInfo
        """
        is_stack = self.tbl.dParseParams.get('is_stack_parsed_cols', False)

        #xxx
        self.tbl.df = self.tbl.df.set_index('Answer Choices')
        self.tbl.idx_col_name = 'Answer Choices'
        print('\n', self.tbl.df)

        if is_stack:
            #xxx
            self.tbl.df = self.tbl.df.stack().reset_index()
            self.tbl.df = self.tbl.df.stack()

            print('\n', self.tbl.df)

            #Respecify the index column name and set default index
            self.tbl.df.columns = [self.tbl.idx_col_name, 'Metric', 'Value']
            self.SetDefaultIndex()

    def ParseBlockProcedure(self):
        """
        Parse the table and set self.df resulting DataFrame
        JDL 9/25/24; Modified 4/22/25
        """
        self.FindFlagEndBound()
        self.ReadHeader()
        self.SubsetDataRows()
        #self.SubsetCols()
        #self.RenameCols()
        #self.SetColumnOrder()

        #Concatenate into tbl.df and re-initialize df_block
        self.tbl.df = pd.concat([self.tbl.df, self.df_block], axis=0)
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

        # Added 4/23 to replace doing this in SubsetCols()
        self.df_block.columns = self.cols_df_block

        # Added 4/23 drop columns with null column name and all null values
        fil = ~self.df_block.columns.isnull() & self.df_block.notna().any()
        self.df_block = self.df_block.loc[:, fil]

    def SubsetCols(self):
        """
        Use tbl.import_col_map to subset columns based on header.
        JDL 9/25/24
        """
        self.df_block.columns = self.cols_df_block

        #Use import_col_map if specified
        if len(self.tbl.import_col_map) > 0:
            cols_keep = list(self.tbl.import_col_map.keys())
            self.df_block = self.df_block[cols_keep]
        
        #Drop columns with blank (e.g. NaN) header
        else:
            self.df_block = self.df_block.dropna(axis=1, how='all')

    def RenameCols(self):
        """
        Optionally use tbl.import_col_map to rename columns.
        JDL 3/4/24; Modified 9/24/24
        """
        if len(self.tbl.import_col_map) > 0:
            self.df_block.rename(columns=self.tbl.import_col_map, inplace=True)

    def SetColumnOrder(self):
        """
        Set column order based on tbl.col_order Series
        JDL 4/22/25
        """
        if self.tbl.col_order is not None:
            self.df_block = self.df_block[self.tbl.col_order]


"""
================================================================================
RowMajorBlockID Class - sub to RowMajorTbl for extracting block_id values
================================================================================
"""
class RowMajorBlockID:
    def __init__(self, parse_instance):
        self.tbl = parse_instance.tbl

        #Index of first data row in .df_raw
        self.idx_start_data = parse_instance.idx_start_data

        #Raw df that is being parsed (.df_raw set in parse_instance.__init__())
        self.df_raw = parse_instance.df_raw

        #Within loop value for a block ID
        self.block_id_value = None

        #List of all block_id names
        self.df_cols_initial = self.tbl.df.columns.tolist()
        self.block_id_names = []

    @property
    def ExtractBlockIDs(self):
        """
        Property returns updated DataFrame and list of names.
        JDL 9/27/24
        """
        self.ExtractBlockIDsProcedure()
        return self.tbl.df, self.block_id_names
    """
    ============================================================================
    """
    def ExtractBlockIDsProcedure(self):
        """
        Procedure to extract block ID values from df_raw based on current block's
        data row index and dict list of block_id tuples: (block_id_name, row_offset,
        col_index) where row_offset is offset from idx_start_data and col_index is 
        absolute column index where each block_id value is found.
        JDL 9/27/24
        """
        #Convert to list if specified as one-item tuple
        self.ConvertTupleToList()

        #Iterate through block_id tuples and add columns to tbl.df
        for tup_block_id in self.tbl.dParseParams.get('block_id_vars', []):
            self.SetBlockIDValue(tup_block_id)
            #self.ReorderColumns() #Don't do as part of parsing -- ApplyColInfo takes care of

    def ConvertTupleToList(self):
        """
        If only one block_id, it can be specified as tuple; otherwise it's
        a list of tuples.
        JDL 9/27/24
        """
        if 'block_id_vars' in self.tbl.dParseParams:

            #If necessary, convert tuple to one-item list
            if isinstance(self.tbl.dParseParams['block_id_vars'], tuple):
                self.tbl.dParseParams['block_id_vars'] = \
                    [self.tbl.dParseParams['block_id_vars']]
            
    def SetBlockIDValue(self, tup_block_id):
        """
        Set internal values based current block_id tuple
        JDL 9/27/24; Modified 4/22/25 for RowMajorTbl refactor
        """
        name, row_offset = tup_block_id[0], tup_block_id[1]
        idx_row, idx_col = self.idx_start_data + row_offset, tup_block_id[2]

        #Set the current value and add the name list
        value_block_id = self.df_raw.iloc[idx_row, idx_col]
        self.block_id_names.append(name)
        self.tbl.df[name] = value_block_id

    def ReorderColumns(self):
        """
        Reorder so that block_id columns are first
        9/27/24
        """
        self.tbl.df = self.tbl.df[self.block_id_names + self.df_cols_initial]

