#Version 5/29/25
#python -m pytest test_parsetables.py -v -s
import sys, os
import pandas as pd
import numpy as np
import pytest
import inspect

# Import the classes to be tested
pf_thisfile = inspect.getframeinfo(inspect.currentframe()).filename
path_libs = os.sep.join(os.path.abspath(pf_thisfile).split(os.sep)[0:-2]) + os.sep + 'libs' + os.sep
if not path_libs in sys.path: sys.path.append(path_libs)

from projfiles import Files
from projtables import ProjectTables
from projtables import Table
from projtables import RowMajorTbl
from projtables import RowMajorBlockID
import parsetables

IsPrint = False

"""
===============================================================================
Test Class projtables.InterleavedColBlocksTbl
===============================================================================
"""
@pytest.fixture
def tbls(files):
    """
    Instance tbls and Promos Table
    """
    tbls = ProjectTables(files)

    # Instance Promos Table with import and parse parameters
    d = {'ftype':'excel', 'sht':'sheet1', 'import_path':files.path_data}
    d['lst_files'] = 'interleaved_test_data.xlsx'
    d2 = {'is_unstructured':True, 'parse_type':'interleaved_col_blocks', \
        'n_cols_metadata':3, 'idx_start':1, 'n_cols_block':2}
    tbls.Promos = Table('Promos', dImportParams=d, dParseParams=d2)
    return tbls

@pytest.fixture
def parse_int(files, tbls):

    # Import the Promos test data
    tbls.Promos.ImportToTblDf()
    
    # Set .df_raw to simulate iteration through list of raw df's
    tbls.Promos.df_raw = tbls.Promos.lst_dfs[0]

    return parsetables.InterleavedColBlocksTbl(tbls.Promos)

"""
===============================================================================
"""
class TestInterleavedColBlocks:
    def test_ParseInterleavedBlocksProcedure(self, parse_int):
        """
        Test - Procedure to parse interleaved blocks of columns
        JDL 3/17/25
        """
        parse_int.ParseInterleavedBlocksProcedure()
        if IsPrint: print('\n', parse_int.df)
        assert len(parse_int.df) == 48

    def test_TransferAllBlocks(self, parse_int):
        """
        Test - TransferAllBlocks method
        """
        parse_int.SetDfMetadata()
        parse_int.DeleteTrailingRows()
        parse_int.TransferAllBlocks()

        # Check that .df has expected number of rows
        assert len(parse_int.df) == 48
        assert parse_int.df.loc[0, 'values'] == 500
        assert parse_int.df.loc[47, 'values'] == 600

    def test_ReadWriteBlock(self, parse_int):
        """
        Test - Read and write a block of columns to .df
        JDL 3/17/25
        """
        parse_int.SetDfMetadata()
        parse_int.DeleteTrailingRows()

        # Set the current column index and call method
        parse_int.idx_col_cur = 4
        parse_int.ReadWriteBlock()

        assert len(parse_int.df) == 24
        assert parse_int.df.loc[0, 'Offer Group Name'] == 'Item 1'
        assert parse_int.df.loc[11, 'Offer Group Name'] == 'Item 12'
        assert parse_int.df.loc[23, 'Offer Group Name'] == 'Item 12'
        assert parse_int.df.loc[0, 'block_name'] == '2021-12-27'
        assert parse_int.df.loc[11, 'block_name'] == '2021-12-27'
        assert parse_int.df.loc[23, 'block_name'] == '2021-12-27'
        assert parse_int.df.loc[0, 'var_name'] == 'Total Units Redeemed'
        assert parse_int.df.loc[11, 'var_name'] == 'Total Units Redeemed'
        assert parse_int.df.loc[12, 'var_name'] == 'Redemption Budget Used'
        assert parse_int.df.loc[23, 'var_name'] == 'Redemption Budget Used'
        assert parse_int.df.loc[0, 'values'] == 500
        assert parse_int.df.loc[11, 'values'] == 30
        assert parse_int.df.loc[12, 'values'] == 5000
        assert parse_int.df.loc[23, 'values'] == 300

    def test_ReadWriteColData(self, parse_int):
        """
        Test - Transfer one column's data to .df by reading from a column block
        JDL 3/17/25
        """
        parse_int.SetDfMetadata()
        parse_int.DeleteTrailingRows()

        # Set the current column index and block name
        parse_int.idx_col_block_cur = 4
        parse_int.block_name_cur = parse_int.df_raw.loc[0, parse_int.idx_col_block_cur]
        parse_int.idx_col_cur = 4
        parse_int.ReadWriteColData()

        assert len(parse_int.df) == 12
        assert parse_int.df.loc[0, 'Offer Group Name'] == 'Item 1'
        assert parse_int.df.loc[11, 'Offer Group Name'] == 'Item 12'
        assert parse_int.df.loc[0, 'block_name'] == '2021-12-27'
        assert parse_int.df.loc[11, 'block_name'] == '2021-12-27'
        assert parse_int.df.loc[0, 'var_name'] == 'Total Units Redeemed'
        assert parse_int.df.loc[11, 'var_name'] == 'Total Units Redeemed'
        assert parse_int.df.loc[0, 'values'] == 500
        assert parse_int.df.loc[11, 'values'] == 30

        # Increment to next column and re-run
        parse_int.idx_col_cur = 5
        parse_int.ReadWriteColData()
        assert len(parse_int.df) == 24
        assert parse_int.df.loc[12, 'var_name'] == 'Redemption Budget Used'
        assert parse_int.df.loc[23, 'var_name'] == 'Redemption Budget Used'
        assert parse_int.df.loc[12, 'values'] == 5000
        assert parse_int.df.loc[23, 'values'] == 300

        # Print the resulting DataFrame
        if IsPrint:
            print('\n', parse_int.df_raw)
            print('\n', parse_int.df)

    def test_DeleteTrailingRows(self, parse_int):
        """
        Delete trailing rows with blank metadata
        JDL 3/17/25
        """
        parse_int.SetDfMetadata()
        parse_int.DeleteTrailingRows()

        assert len(parse_int.df_metadata) == 12
        assert parse_int.df_metadata.loc[11, 'Offer Group Name'] == 'Item 12'
        assert len(parse_int.df_raw) == 14
        assert parse_int.df_raw.loc[13, 1] == 'Item 12'

    def test_SetDfMetadata(self, parse_int):
        """
        Set .df_metadata as a subset of .df_raw
        JDL 3/17/25
        """
        parse_int.SetDfMetadata()

        # Check that .df_metadata columns and rows
        cols = ['Offer Group Name', 'Platform Comparison', 'Retailer Name']

        # Print the  metadata DataFrame
        if IsPrint: print('\n', parse_int.df_metadata)

        assert list(parse_int.df_metadata.columns) == cols
        assert len(parse_int.df_metadata) == 13

    def test_parse_int_fixture(self, parse_int):
        """
        Test - Import Excel raw interleaved data
        JDL 3/17/25
        """
        # Print the raw DataFrame
        if IsPrint: print('\n', parse_int.df_raw)

        assert parse_int.df_raw.index.size == 15

"""
Stop 13:30 9/27/24
Committed working code with block_id vars
Ready to add mods to Stack procedure to account for block_id vars
Should test that on survey data
"""
"""
================================================================================
Importing/Parsing Raw Data with ProjectTables class 
================================================================================
"""
subdir_tests = 'test_data_parse'

@pytest.fixture
def files():
    return Files('tbls', IsTest=True, subdir_tests=subdir_tests)

"""
================================================================================
RowMajorTbl Class - for parsing row major raw data
(Survey Monkey report format)
================================================================================
"""
@pytest.fixture
def dParseParams_tbl1_survey():
    """
    Parameters for parsing survey data
    JDL Modified 4/22/25
    """
    dParseParams = {}
    dParseParams['is_unstructured'] = True
    dParseParams['parse_type'] = 'row_major'
    dParseParams['import_dtype'] = str
    dParseParams['flag_start_bound'] = 'Answer Choices'
    dParseParams['flag_end_bound'] = '<blank>'
    dParseParams['icol_start_bound'] = 0
    dParseParams['icol_end_bound'] = 0
    dParseParams['iheader_rowoffset_from_flag'] = 0
    dParseParams['idata_rowoffset_from_flag'] = 1
    return dParseParams

@pytest.fixture
def tbl1_survey(files, dParseParams_tbl1_survey):
    """
    Table object for survey data
    JDL 9/25/24; Modified 5/29/25
    """
    dImport = {'ftype':'excel', 'import_path':files.path_data,
        'sht':'raw_table', 'lst_files':'tbl1_survey.xlsx'}
    tbl = Table('tbl1_survey', dImportParams=dImport, \
        dParseParams=dParseParams_tbl1_survey)
    tbl.ImportToTblDf()
    return tbl

@pytest.fixture
def row_maj_tbl1_survey(tbl1_survey):
    """
    Instance RowMajorTbl parsing class for survey data
    JDL 9/25/24
    """
    return parsetables.RowMajorTbl(tbl1_survey, df=tbl1_survey.lst_dfs[0])

"""
================================================================================
"""
class TestParseRowMajorTbl1Survey:
    
    def test_survey_ReadBlocksProcedure1(self, row_maj_tbl1_survey):
        """
        Procedure to iteratively parse row major blocks
        (parse a raw table containing two blocks)
        JDL 9/26/24
        """
        row_maj_tbl1_survey.ReadBlocksProcedure()

        #Check that procedure found three blocks
        assert row_maj_tbl1_survey.start_bound_indices == [3, 14, 24]
        assert len(row_maj_tbl1_survey.tbl.df) == 11

        df_check = row_maj_tbl1_survey.tbl.df

        if IsPrint: print('\n\n', row_maj_tbl1_survey.tbl.df, '\n')

        lst_expected = ['Daily', '14.13%', '76', np.nan, np.nan, np.nan]
        check_series_values(df_check.iloc[0], lst_expected)
        lst_expected =  ['Improved cleaning', np.nan, np.nan, '18', '11', '17']
        check_series_values(df_check.iloc[-1], lst_expected)

        if False: print_tables(row_maj_tbl1_survey)

    def xtest_survey_ReadBlocksProcedure2(self, row_maj_tbl1_survey):
        """
        ===Move to ApplyColInfo===
        Procedure to iteratively parse row major blocks
        (parse a raw table containing two blocks)
        Stack the parsed data
        JDL 9/25/24
        """
        row_maj_tbl1_survey.tbl.dParseParams['is_stack_parsed_cols'] = True
        row_maj_tbl1_survey.ReadBlocksProcedure()

        assert len(row_maj_tbl1_survey.tbl.df) == 11

        if IsPrint: print('\n\n', row_maj_tbl1_survey.tbl.df, '\n')

        #===remove StackParsedCols to move to ApplyColInfo===
        # Check values in first two and last two rows
        #df_check = row_maj_tbl1_survey.tbl.df.reset_index(drop=False)
        #lst_expected = ['Daily', 'Response Percent', '14.13%']
        #check_series_values(df_check.iloc[0], lst_expected)
        #lst_expected = ['Daily', 'Responses', '76']
        #check_series_values(df_check.iloc[1], lst_expected)

        #lst_expected =  ['Improved cleaning', '3', '17']
        #check_series_values(df_check.iloc[-1], lst_expected)
        #lst_expected =  ['Improved cleaning', '2', '11']
        #check_series_values(df_check.iloc[-2], lst_expected)

        if False: print_tables(row_maj_tbl1_survey)

    def test_survey_ParseBlockProcedure1(self, row_maj_tbl1_survey):
        """
        Parse the survey table and check the final state of the table.
        (1st block)
        JDL 9/25/24
        """

        SetListFirstStartBoundIndex(row_maj_tbl1_survey)
        row_maj_tbl1_survey.ParseBlockProcedure()

        #Check resulting .tbl.df relative to tbl1_survey.xlsx
        assert len(row_maj_tbl1_survey.tbl.df) == 5

        if IsPrint: print('\n\n', row_maj_tbl1_survey.tbl.df, '\n')

        assert list(row_maj_tbl1_survey.tbl.df.iloc[0]) == ['Daily', '14.13%', '76']
        assert list(row_maj_tbl1_survey.tbl.df.iloc[-1]) == ['Rarely', '0.37%', '2']

        if False: print_tables(row_maj_tbl1_survey)

    def test_survey_ParseBlockProcedure2(self, row_maj_tbl1_survey):
        """
        Parse the survey table and check the final state of the table.
        JDL 9/25/24; Modified 9/26/24
        """
        #Add a trailing blank row to .df_raw (last <blank> end flag)
        row_maj_tbl1_survey.AddTrailingBlankRow()

        #Set current start bound index to be last (third) block
        row_maj_tbl1_survey.SetStartBoundIndices()
        row_maj_tbl1_survey.idx_start_current = \
            row_maj_tbl1_survey.start_bound_indices[-1]
        assert row_maj_tbl1_survey.idx_start_current == 24

        row_maj_tbl1_survey.ParseBlockProcedure()

        if IsPrint: print('\n\n', row_maj_tbl1_survey.tbl.df, '\n')

        #Check resulting .tbl.df relative to tbl1_survey.xlsx
        assert len(row_maj_tbl1_survey.tbl.df) == 3
        assert list(row_maj_tbl1_survey.tbl.df.iloc[0]) == ['Lower price point', '91', '33', '19']
        assert list(row_maj_tbl1_survey.tbl.df.iloc[-1]) == ['Improved cleaning', '18', '11', '17']

        if False: print_tables(row_maj_tbl1_survey)

    def xtest_survey_SubsetCols(self, row_maj_tbl1_survey):
        """
        ===Move to ApplyColInfo===
        Use tbl.import_col_map to subset columns based on header.
        JDL 9/24/24
        """
        SetListFirstStartBoundIndex(row_maj_tbl1_survey)
        row_maj_tbl1_survey.FindFlagEndBound()
        row_maj_tbl1_survey.ReadHeader()
        row_maj_tbl1_survey.SubsetDataRows()
        row_maj_tbl1_survey.SubsetCols()

        # Assert that column names are correct before renaming
        lst_expected = ['Answer Choices', 'Response Percent', 'Responses']
        assert list(row_maj_tbl1_survey.df_block.columns) == lst_expected

    def test_survey_SubsetDataRows(self, row_maj_tbl1_survey):
        """
        Subset rows based on flags and idata_rowoffset_from_flag.
        JDL 9/24/24
        """
        SetListFirstStartBoundIndex(row_maj_tbl1_survey)
        row_maj_tbl1_survey.FindFlagEndBound()
        row_maj_tbl1_survey.ReadHeader()
        row_maj_tbl1_survey.SubsetDataRows()

        # Check resulting .tbl.df relative to tbl1_raw.xlsx
        assert len(row_maj_tbl1_survey.df_block) == 5
        lst_expected = ['Daily', '14.13%', '76', None, None, None]
        check_series_values(row_maj_tbl1_survey.df_block.iloc[0], lst_expected)

        lst_expected = ['Rarely', '0.37%', '2', None, None, None]
        check_series_values(row_maj_tbl1_survey.df_block.iloc[-1], lst_expected)

    def test_survey_ReadHeader(self, row_maj_tbl1_survey):
        """
        Read header based on iheader_rowoffset_from_flag.
        JDL 9/24/24
        """
        SetListFirstStartBoundIndex(row_maj_tbl1_survey)
        row_maj_tbl1_survey.FindFlagEndBound()
        row_maj_tbl1_survey.ReadHeader()

        # Assert that the header row index was set correctly
        assert row_maj_tbl1_survey.idx_header_row == 3

        #  Check each column name, allowing for NaN comparisons
        lst_expected = ['Answer Choices', 'Response Percent', 'Responses', None, None, None]
        check_series_values(row_maj_tbl1_survey.cols_df_block, lst_expected)

    def test_survey_FindFlagEndBound(self, row_maj_tbl1_survey):
        """
        Find index of flag_end_bound row
        JDL 9/25/24
        """
        #Locate the start bound indices and truncate to just first
        SetListFirstStartBoundIndex(row_maj_tbl1_survey)
        assert row_maj_tbl1_survey.idx_start_current == 3

        row_maj_tbl1_survey.FindFlagEndBound()
        assert row_maj_tbl1_survey.idx_end_bound == 9

    def test_survey_SetStartBoundIndices(self, row_maj_tbl1_survey):
        """
        Populate .start_bound_indices list of row indices where
        flag_start_bound is found
        JDL 9/25/24
        """
        row_maj_tbl1_survey.SetStartBoundIndices()

        # Expected indices where 'Answer Choices' is found
        expected_indices = [3, 14, 24]

        assert row_maj_tbl1_survey.start_bound_indices == expected_indices

    def test_survey_AddTrailingBlankRow(self, row_maj_tbl1_survey):
        """
        Add a trailing blank row to self.df_raw (to ensure last <blank> flag to
        terminate last block)
        JDL 9/26/24
        """
        assert row_maj_tbl1_survey.df_raw.shape == (28, 4)
        row_maj_tbl1_survey.AddTrailingBlankRow()
        assert row_maj_tbl1_survey.df_raw.shape == (29, 4)

class TestTbl1SurveyFixtures:
    """ Fixtures for tbl1_survey """

    def test_survey_row_maj_fixture(self, row_maj_tbl1_survey):    
        """
        Test that raw survey data imported correctly
        JDL 9/24/24
        """
        assert row_maj_tbl1_survey.df_raw.shape == (28, 4)

    def test_survey_tbl1_fixture(self, tbl1_survey):
        """
        Test that survey data imported correctly
        JDL 9/25/24
        """
        assert tbl1_survey.lst_dfs[0].shape == (28, 4)

"""
================================================================================
RowMajorTbl Class - for parsing row major raw data
Example with one block
================================================================================
"""
@pytest.fixture
def dParseParams_tbl1():
    """
    Return a dictionary of parameters for parsing tbl1
    """
    dParseParams = {}
    dParseParams['is_unstructured'] = True
    dParseParams['parse_type'] = 'row_major'
    dParseParams['import_dtype'] = str
    dParseParams['flag_start_bound'] = 'flag'
    dParseParams['flag_end_bound'] = '<blank>'
    dParseParams['icol_start_bound'] = 1
    dParseParams['icol_end_bound'] = 2
    dParseParams['iheader_rowoffset_from_flag'] = 1
    dParseParams['idata_rowoffset_from_flag'] = 2

    #Specify one item tuple to extract a block ID value from above the block
    dParseParams['block_id_vars'] = ('stuff', -4, 2)

    return dParseParams

@pytest.fixture
def tbl1(files, dParseParams_tbl1):
    """
    Table object for example data
    JDL 9/25/24; Modified 4/21/25
    """
    # Instance Table
    dImport = {'ftype':'excel',
        'import_path':files.path_data,
        'sht':'raw_table',
        'lst_files':'tbl1_raw.xlsx'}
    tbl = Table('tbl1_raw', dImportParams=dImport, dParseParams=dParseParams_tbl1)
    tbl.ImportToTblDf()
    return tbl

@pytest.fixture
def row_maj_tbl1(tbl1):
    """
    Instance RowMajorTbl parsing class for Table1 data
    (df argument simulates iteration df from .lst_dfs)
    JDL 9/26/24; Modified 4/21/25
    """
    parse = parsetables.RowMajorTbl(tbl1, df=tbl1.lst_dfs[0])
    parse.tbl.import_col_map = \
            {'idx_raw':'idx', 'col #1':'col_1', 'col #2':'col_2'}
    parse.tbl.col_order = pd.Series(index=[0, 1, 2], data=['idx', 'col_2', 'col_1'])
    return parse

"""
================================================================================
"""
class TestParseRowMajorTbl1:
    """row_major parsing of single block from Excel sheet"""

    def test_ReadBlocksProcedure(self, row_maj_tbl1):
        """
        Procedure to iteratively parse row major blocks
        (parse a raw table containing one block)
        JDL 9/26/24; Modified 4/21/25
        """
        row_maj_tbl1.ReadBlocksProcedure()

        #Check the final state of the table
        self.check_tbl1_values(row_maj_tbl1)

        if False: print_tables(row_maj_tbl1)

    def test_ParseBlockProcedure(self, row_maj_tbl1):
        """
        Parse an individual block
        JDL 4/22/25 added to aid debug ExtractBlockIDs
        """
        row_maj_tbl1.AddTrailingBlankRow()
        row_maj_tbl1.SetStartBoundIndices()

        # Set first (only for tbl1) index as current and Parse the block
        row_maj_tbl1.idx_start_current = row_maj_tbl1.start_bound_indices[0]
        row_maj_tbl1.ParseBlockProcedure()

        assert row_maj_tbl1.tbl.df.shape == (5, 3)
        assert row_maj_tbl1.df_raw.shape == (14, 5)
    
    def xtest_SetDefaultIndex(self, row_maj_tbl1):
        """
        ===Move to ApplyColInfo===
        Set default index and check the final state of the table.
        JDL 3/4/24; Modified 4/21/25
        """
        # Precursor methods (ReadBlocksProcedure)
        row_maj_tbl1.AddTrailingBlankRow()
        row_maj_tbl1.SetStartBoundIndices()
        for i in row_maj_tbl1.start_bound_indices:
            row_maj_tbl1.idx_start_current = i
            row_maj_tbl1.ParseBlockProcedure()

        row_maj_tbl1.SetDefaultIndex()

        #Extract block_id value specified in dParseParams
        row_maj_tbl1.tbl.df, row_maj_tbl1.lst_block_ids = \
            RowMajorBlockID(row_maj_tbl1.tbl, row_maj_tbl1.idx_start_data).ExtractBlockIDs
        
        #Check the final state of the table
        check_tbl1_values(row_maj_tbl1)

        if False: print_tables(row_maj_tbl1)

    def check_tbl1_values(self, row_maj_tbl1):
        """
        Check the final state of the table (ApplyColInfo sets data types)
        JDL 3/4/24; Modified 4/22/25
        """
        #Check index name and column names 
        assert list(row_maj_tbl1.tbl.df.columns) == ['idx_raw', 'col #1', 'col #2', 'stuff']

        #Check resulting .tbl.df relative to tbl1_raw.xlsx (import raw to str dtype)
        assert len(row_maj_tbl1.tbl.df) == 5
        assert list(row_maj_tbl1.tbl.df.loc[0]) == ['1', '10', 'a', 'Stuff in C']
        assert list(row_maj_tbl1.tbl.df.loc[4]) == ['5', '50', 'e', 'Stuff in C']

    def test_SubsetDataRows(self, row_maj_tbl1):
        """
        Subset raw data rows based on flags and idata_rowoffset_from_flag
        JDL 3/4/24; Modified 9/26/24; Modified 4/21/25
        """
        #Locate the start bound idx    
        row_maj_tbl1.SetStartBoundIndices()
        row_maj_tbl1.idx_start_current = row_maj_tbl1.start_bound_indices[0]

        # Block specific methods
        row_maj_tbl1.FindFlagEndBound()
        row_maj_tbl1.ReadHeader()
        row_maj_tbl1.SubsetDataRows()

        # Check resulting .tbl.df relative to tbl1_raw.xlsx
        assert len(row_maj_tbl1.df_block) == 5

        if IsPrint: print('\n\n', row_maj_tbl1.df_block, '\n')

        lst_expected = ['1', '10', 'a']
        check_series_values(row_maj_tbl1.df_block.iloc[0], lst_expected)

        lst_expected = ['5', '50', 'e']
        check_series_values(row_maj_tbl1.df_block.iloc[-1], lst_expected)

    def test_ReadHeader(self, row_maj_tbl1):
        """
        Read header based on iheader_rowoffset_from_flag.
        JDL 3/4/24; Modified 9/26/24; Modified 4/21/25
        """
        #Locate the start bound idx    
        row_maj_tbl1.SetStartBoundIndices()
        row_maj_tbl1.idx_start_current = row_maj_tbl1.start_bound_indices[0]

        # Block specific methods
        row_maj_tbl1.FindFlagEndBound()
        row_maj_tbl1.ReadHeader()

        # Check header row index was set correctly
        assert row_maj_tbl1.idx_header_row == 5

        # Check block's column names
        lst_expected = [None, None, 'idx_raw', 'col #1', 'col #2']
        check_series_values(row_maj_tbl1.cols_df_block, lst_expected)

    def test_FindFlagEndBound(self, row_maj_tbl1):
        """
        Find index of flag_end_bound row
        JDL 3/4/24; Modified 9/26/24; Modified 4/21/25
        """
        #Locate the start bound idx    
        row_maj_tbl1.SetStartBoundIndices()
        row_maj_tbl1.idx_start_current = row_maj_tbl1.start_bound_indices[0]

        # Call the method and check result for tbl1_raw.xlsx
        row_maj_tbl1.FindFlagEndBound()
        assert row_maj_tbl1.idx_end_bound == 11

    def test_SetStartBoundIndices(self, row_maj_tbl1):
        """
        Populate .start_bound_indices list of row indices where
        flag_start_bound is found
        JDL 9/26/24; Modified 4/21/25
        """
        row_maj_tbl1.SetStartBoundIndices()

        # Expected indices where 'flag' is found
        expected_indices = [4]

        assert row_maj_tbl1.start_bound_indices == expected_indices

    def test_AddTrailingBlankRow(self, row_maj_tbl1):
        """
        Add a trailing blank row to self.df_raw (to ensure last <blank> flag to
        terminate last block)
        JDL 9/26/24; Modified 4/21/25
        """
        assert row_maj_tbl1.df_raw.shape == (13, 5)
        row_maj_tbl1.AddTrailingBlankRow()
        assert row_maj_tbl1.df_raw.shape == (14, 5)

class TestTbl1Fixtures:
    """RowMajorTbl parsing test fixtures"""
    
    def test_row_maj_tbl1(self, row_maj_tbl1, tbl1):
        """
        Test that RowMajorTbl object created and .df_raw populated
        JDL 4/21/25
        """
        assert row_maj_tbl1.df_raw.shape == (13, 5)
        check_series_values(row_maj_tbl1.df_raw.iloc[0], 5*[None])

        check_series_values(tbl1.lst_dfs[0].iloc[0], 5*[None])

    def test_tbl1(self, tbl1):
        """
        Test that Table1 data imported correctly
        JDL 9/26/24; Modified 4/21/25
        """
        assert len(tbl1.lst_dfs) == 1
        assert tbl1.lst_dfs[0].shape == (13, 5)

    def test_files(self, files):
        """
        Test that the files object was created correctly
        JDL 9/24/24; Modified 4/21/25
        """
        assert files.path_data.split(os.sep)[-3:] == ['tests', 'test_data_parse', '']
        assert files.path_libs.split(os.sep)[-2:] == ['libs', '']
        assert files.path_root.split(os.sep)[-2:] == ['Python_ModelingToolbox', '']
        assert files.path_tests.split(os.sep)[-2:] == ['tests', '']

"""
================================================================================
RowMajorBlockID Class - sub to RowMajorTbl for extracting block_id values
JDL 9/27/24
================================================================================
"""
class TestExtractBlockIDs:
    def test_blockids_ExtractBlockIDsProcedure1(self, row_maj_tbl1):
        """
        Procedure to extract block ID values from df_raw
        (Each Block_ID variable input as tuple in tbl.dParseParams['block_id_vars'])
        BlockID's are individual values located relative to first row of data in bloc
        JDL 9/27/24
        """
        #Parse the block's row major data to populate .tbl.df
        self.create_tbl_df(row_maj_tbl1)

        #Call property to extract block IDs
        df, lst = RowMajorBlockID(row_maj_tbl1).ExtractBlockIDs

        assert lst == ['stuff']
        assert len(df) == 5
        assert list(df.columns) == ['idx_raw', 'col #1', 'col #2', 'stuff']

    def test_blockids_ExtractBlockIDsProcedure2(self, row_maj_tbl1):
        """
        Procedure to extract block ID values from df_raw
        (Two Block_ID variables input as list)
        JDL 9/27/24
        """
        #Parse the block's row major data to populate .tbl.df
        self.create_tbl_df(row_maj_tbl1)

        #Override default parsing instruction
        lst = [('stuff', -4, 2), ('stuff2', -2, 1)]
        row_maj_tbl1.tbl.dParseParams['block_id_vars'] = lst
            
        #Call property to extract block IDs
        df, lst = RowMajorBlockID(row_maj_tbl1).ExtractBlockIDs

        assert lst == ['stuff', 'stuff2']
        assert len(df) == 5
        assert list(df.columns) == ['idx_raw', 'col #1', 'col #2', 'stuff', 'stuff2']
        assert all(df['stuff'] == 'Stuff in C') 
        assert all(df['stuff2'] == 'flag') 

    def xtest_blockids_ReorderColumns(self, row_maj_tbl1):
        """
        ===Move to ApplyColInfo (one column reorder based on tbl.col_order)===
        If only one block_id, it can be specified as tuple; otherwise it's
        a list of tuples.
        JDL 9/27/24
        """
        #Parse the block's row major data to populate .tbl.df
        create_tbl_df(row_maj_tbl1)

        #Instance of RowMajorBlockID
        row_maj_block_id = RowMajorBlockID(row_maj_tbl1.tbl, row_maj_tbl1.idx_start_data)

        #Call methods
        row_maj_block_id.ConvertTupleToList()
        row_maj_block_id.SetBlockIDValue(row_maj_tbl1.tbl.dParseParams['block_id_vars'][0])
        row_maj_block_id.ReorderColumns()
        assert list(row_maj_block_id.tbl.df.columns) == ['stuff', 'idx', 'col_1', 'col_2']

    def test_SetBlockIDValue(self, row_maj_tbl1):
        """
        If only one block_id, it can be specified as tuple; otherwise it's
        a list of tuples.
        JDL 9/27/24; Modified/revalidated 4/22/25
        """
        #Parse the block's row major data to populate .tbl.df
        self.create_tbl_df(row_maj_tbl1)

        #Instance of RowMajorBlockID
        row_maj_block_id = RowMajorBlockID(row_maj_tbl1)
        row_maj_block_id.ConvertTupleToList()

        # use first/only block_id var for testing; Set block_id value
        var_current = row_maj_tbl1.tbl.dParseParams['block_id_vars'][0]
        row_maj_block_id.SetBlockIDValue(var_current)

        assert row_maj_block_id.block_id_names == ['stuff']
        assert list(row_maj_block_id.tbl.df.columns) == ['idx_raw', 'col #1', 'col #2', 'stuff']
        assert all(row_maj_block_id.tbl.df['stuff'] == 'Stuff in C') 

    def create_tbl_df(self, row_maj_tbl):
        """
        Helper function to parse raw data to row_maj_tbl.tbl.df
        JDL 9/27/24
        """
        # Set idx_start_current to first list item
        SetListFirstStartBoundIndex(row_maj_tbl)

        # Parse that block and check that it got parsed
        row_maj_tbl.ParseBlockProcedure()
        assert len(row_maj_tbl.tbl.df) == 5

    def test_ConvertTupleToList(self, tbl1, row_maj_tbl1):
        """
        If only one block_id, it can be specified as tuple; otherwise it's
        a list of tuples.
        JDL 9/27/24
        """
        #instance of RowMajorBlockID --arg needs to be parse object aka RowMajorTbl
        row_maj_block_id = RowMajorBlockID(row_maj_tbl1)

        #Check input from dParseParams fixture
        assert isinstance(tbl1.dParseParams['block_id_vars'], tuple)

        #Call method and check conversion to list
        row_maj_block_id.ConvertTupleToList()
        assert isinstance(tbl1.dParseParams['block_id_vars'], list)

"""
================================================================================
Helper methods for testing
================================================================================
"""
def print_tables(row_maj_tbl1_survey):
    """
    Helper function to print raw and parsed tables
    JDL 9/25/24
    """
    print('\n\nraw imported table\n', row_maj_tbl1_survey.df_raw)
    print('\nparsed table\n', row_maj_tbl1_survey.tbl.df, '\n\n')

def check_series_values(ser, lst_expected):
    """
    Helper function to check series values allowing for NaN comparisons
    JDL 9/25/24
    """
    for actual, expect in zip(ser, lst_expected):
        if isinstance(expect, float) and np.isnan(expect):
            assert np.isnan(actual)
        else:
            assert actual == expect

def SetListFirstStartBoundIndex(row_maj_tbl1_survey):
    """
    Helper test function - set .idx_start_current to first list item
    JDL 9/25/24
    """
    row_maj_tbl1_survey.SetStartBoundIndices()
    row_maj_tbl1_survey.idx_start_current = \
        row_maj_tbl1_survey.start_bound_indices[0]

