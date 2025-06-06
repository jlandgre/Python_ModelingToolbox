#Version 5/29/25
#python -m pytest test_parsetables.py -v -s
import sys, os
import pandas as pd
import numpy as np
import pytest
import inspect
import datetime as dt

# Import the classes to be tested
pf_thisfile = inspect.getframeinfo(inspect.currentframe()).filename
path_libs = os.sep.join(os.path.abspath(pf_thisfile).split(os.sep)[0:-2]) + os.sep + 'libs' + os.sep
if not path_libs in sys.path: sys.path.append(path_libs)

from projfiles import Files
from projtables import ProjectTables
from projtables import Table
import parsetables

IsPrint = False

"""
===============================================================================
Test Class projtables.Table.ParseRawData (JDL 5/30/25)
The ParseRawData procedure operates on Table.lst_dfs created by .ImportToTblDf 
when .is_unstructured=True. In this case, the import leaves the unparsed raw
data as dfs in .lst_dfs. ParseRawData iterates through the list of raw dfs and
applies the specified (.dParseParams['parse_type']) parsing procedure to each
===============================================================================
"""
@pytest.fixture
def tbls_both(files):
    """
    Instance tbls with Promos and Survey Tables
    """
    tbls_both = ProjectTables(files)

    # Instance Promos Table with import and parse parameters
    d = {'ftype':'excel', 'sht':'sheet1', 'import_path':files.path_data}
    #d['lst_files'] = 'interleaved_test_data.xlsx'
    d2 = {'is_unstructured':True, 
        'import_dtype': str,
        'parse_type':'InterleavedColBlocksTbl',
        'n_cols_metadata':3, 
        'idx_start':1, 
        'n_cols_block':2}
    tbls_both.Promos = Table('Promos', dImportParams=d, dParseParams=d2)

    # Instance survey Table with import and parse parameters including block_id_var
    d = {'ftype':'excel', 'import_path':files.path_data, 'sht':'raw_table'}
    d2 = {'is_unstructured': True, 
        'parse_type': 'RowMajorTbl', 
        'import_dtype': str,
        'flag_start_bound': 'Answer Choices',
        'flag_end_bound': '<blank>',
        'icol_start_bound': 0,
        'icol_end_bound': 0,
        'iheader_rowoffset_from_flag': 0,
        'idata_rowoffset_from_flag': 1,
        'block_id_vars': ('question_text', -2, 0)}
    tbls_both.Survey = Table('tbl1_survey', dImportParams=d, dParseParams=d2)
    return tbls_both

class TestParseRawData:
    def test_ParseRawData1(self, tbls_both):
        """
        ParseRawData for Promos Table (InterleavedColBlocksTbl)
        JDL 5/30/25
        """
        # Import and parse the Promos test data
        tbls_both.Promos.ImportToTblDf(lst_files='interleaved_test_data.xlsx')
        tbls_both.Promos.ParseRawData()

        # Check resulting .df
        df = tbls_both.Promos.df
        assert len(df) == 48

        # Check Repeating blocks and sum of values from hand calculation
        assert list(df['Offer Group Name']) == 4 *  [f'Item {i}' for i in range(1, 13)]
        assert list(df['block_name']) == 24 * ['2021-12-27'] + 24 * ['2022-01-03']
        assert list(df['var_name']) == \
            2* (12 * ['Total Units Redeemed'] + 12 * ['Redemption Budget Used'])
        assert df['values'].astype('float64').sum() == 63360.

        if IsPrint:
            print(f'\nRaw df\n{tbls_both.Promos.lst_dfs[0]}')
            print(f'\nParsed df\n{tbls_both.Promos.df}\n\n')
            print(tbls_both.Promos.df.info(), '\n')

    def test_ParseRawData2(self, tbls_both):
        """
        ParseRawData for Survey Table (RowMajorTbl)
        JDL 5/30/25
        """
        # Import and parse the Survey test data
        tbls_both.Survey.ImportToTblDf(lst_files='tbl1_survey.xlsx')

        #for i, df in enumerate(tbls_both.Survey.lst_dfs):
        #    print(f'\nRaw df {i}\n{df}')

        tbls_both.Survey.ParseRawData()

        print('\n\n', tbls_both.Survey.df, '\n')

        # Check resulting .df
        df = tbls_both.Survey.df
        assert len(df) == 11
        lst = ['question_text', 'Answer Choices', 'Response Percent', 'Responses', '1', '2', '3']
        assert list(df.columns) == lst
        assert list(df['question_text']) == 5 *  ['Q1. How often do you wash your car?'] + \
                            3 * ['Q2. What brands of car wash cleaner do you use'] + \
                            3 * ['Q3. How would you improve your current product (Rank 1 to 3)']

        # Check first and last row values                   
        cols = ['Answer Choices', 'Response Percent', 'Responses', '1', '2', '3']
        assert list(df.iloc[0][cols]) == ['Daily', '14.13%', '76', np.nan, np.nan, np.nan]
        assert list(df.iloc[-1][cols]) == ['Improved cleaning', np.nan, np.nan,'18', '11', '17']

        if IsPrint:
            print(f'\nRaw df\n{tbls_both.Survey.lst_dfs[0]}')
            print(f'\nParsed df\n{tbls_both.Survey.df}\n\n')
            print(tbls_both.Survey.df.info(), '\n')

"""
===============================================================================
Test Class parsefiles.ParseColMajorTbl (6/6/25)
===============================================================================
"""
@pytest.fixture
def tbls_cm(files):
    """
    Instance tbls and ColMajor Table
    """
    tbls = ProjectTables(files)

    # Instance a Table with import params for test_data_parse file and parse params
    d = {'ftype':'excel', 'sht':'Sheet1', 'import_path':files.path_data}
    d['lst_files'] = 'col_major_test_data.xlsx'

    # First two keys for completeness; not used in this test
    d2 = {'is_unstructured':True, 
        'parse_type':'ParseColMajorTbl',
        'flag_start_bound': 'Total Orders',
        'flag_end_bound': 'Total',
        'icol_start_flag': 0,      # 'Total Orders' is in column 0
        'icol_end_flag': 0,        # 'Total' is in column 0
        'nrows_header_offset_from_flag': 1,   # header row is 1 row below start flag
        'nrows_data_offset_from_flag': 2,     # data starts 2 rows below start flag
        'nrows_data_end_offset_from_flag': -1}  # data ends at the row above 'Total'
    
    tbls.ColMajor = Table('ColMajor', dImportParams=d, dParseParams=d2)
    return tbls

@pytest.fixture
def parse_cm(files, tbls_cm):
    """
    Import test data and instance ParseColMajorTbl class
    JDL 6/6/25
    """
    # Import the test data to .lst_dfs and set .df_raw to sim iteration
    tbls_cm.ColMajor.ImportToTblDf()
    tbls_cm.ColMajor.df_raw = tbls_cm.ColMajor.lst_dfs[0]
    return parsetables.ParseColMajorTbl(tbls_cm.ColMajor)

class TestParseColMajorTbl:
    def test_ParseDfRawProcedure_ColMajor(self, parse_cm):
        """
        Procedure to parse blocks of columns in self.df_raw and set self.df
        JDL 6/6/25
        """
        parse_cm.ParseDfRawProcedure()
        assert parse_cm.df.shape[0] == 6

        if IsPrint:
            print('\n', parse_cm.df, '\n')
            print(parse_cm.df.info())

    def test_TransferAllCols(self, parse_cm):
        """
        Iterate over data columns and transfer their data to self.df.
        """
        parse_cm.FindDataBoundaries()
        parse_cm.SetDfCategories()
        parse_cm.TransferAllCols()
        assert parse_cm.df.shape[0] == 6
        assert parse_cm.df['category'].tolist() == 3 * ['category1', 'category2']
        assert parse_cm.df['value'].tolist() == [10, 50, 20, 60, 30, 70]
        
        # Set type to dates to check
        parse_cm.df['col_header'] = parse_cm.df['col_header'].dt.date
        expected = 2 * [dt.date(2017,9,2)] + 2 * [dt.date(2017,9,9)] + 2 * [dt.date(2017,9,16)]
        assert parse_cm.df['col_header'].tolist() == expected

    def test_ReadWriteColData(self, parse_cm):
        """
        Add one date column's data to self.df using self.idx_col_cur.
        """
        # Run precursor methods
        parse_cm.FindDataBoundaries()
        parse_cm.SetDfCategories()

        # Set idx_col_cur to 1 (first date column in test data)
        parse_cm.idx_col_cur = 1
        parse_cm.ReadWriteColData()

        # Check that .df has 2 rows and correct columns
        assert parse_cm.df.shape[0] == 2
        assert list(parse_cm.df.columns) == ['col_header', 'category', 'value']

        # Check that categories and values are as expected
        assert parse_cm.df['category'].tolist() == ['category1', 'category2']
        assert parse_cm.df['value'].tolist() == [10, 50]

    def test_SetDfCategories(self, parse_cm):
        """
        Set list of categories from the first column between data start and end
        (limited by hard-coded column 0; should refactor use use dParseParams to set attribute)
        JDL 6/6/25
        """
        parse_cm.FindDataBoundaries()
        parse_cm.SetDfCategories()
        assert parse_cm.lstCategories == ['category1', 'category2']
        
    def test_FindDataBoundaries(self, parse_cm):
        """
        Set data boundary indices for parsing using parse params and flag columns.
        JDL 6/6/25
        """
        parse_cm.FindDataBoundaries()
        assert parse_cm.idx_header_row == 4
        assert parse_cm.idx_data_start == 5
        assert parse_cm.idx_data_end == 6

    def test_ColMajor_init(self, parse_cm, tbls_cm):
        """
        Parse data in columns with categories in column 0 rows
        JDL 6/6/25
        """
        # Check df_raw is set and output is empty
        assert parse_cm.df_raw is not None
        assert parse_cm.df.empty

        # Check flag and col index attributes
        assert parse_cm.flag_start == tbls_cm.ColMajor.dParseParams['flag_start_bound']
        assert parse_cm.flag_end == tbls_cm.ColMajor.dParseParams['flag_end_bound']
        assert parse_cm.idx_start_flag_col == tbls_cm.ColMajor.dParseParams['icol_start_flag']
        assert parse_cm.idx_end_flag_col == tbls_cm.ColMajor.dParseParams['icol_end_flag']

        # Check row offset attributes
        assert parse_cm.header_row_offset == tbls_cm.ColMajor.dParseParams['nrows_header_offset_from_flag']
        assert parse_cm.data_start_row_offset == tbls_cm.ColMajor.dParseParams['nrows_data_offset_from_flag']
        assert parse_cm.data_end_row_offset == tbls_cm.ColMajor.dParseParams['nrows_data_end_offset_from_flag']

    def test_parse_cm_fixture(self, parse_cm):
        """
        parse_cm fixture imports the raw data and sets .df_raw
        JDL 6/6/25
        """
        assert parse_cm.df_raw.shape == (8, 4)

"""
===============================================================================
Test Class parsefiles.InterleavedColBlocksTbl
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
    d2 = {'is_unstructured':True, 'parse_type':'InterleavedColBlocksTbl', \
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


class TestInterleavedColBlocks:
    def test_ParseDfRawProcedure(self, parse_int):
        """
        Test - Procedure to parse interleaved blocks of columns
        JDL 3/17/25
        """
        parse_int.ParseDfRawProcedure()
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
    dParseParams['parse_type'] = 'RowMajorTbl'
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
    JDL 9/25/24; Modified 5/30/25
    """
    d = {'ftype':'excel', 'import_path':files.path_data,'sht':'raw_table'}
    tbl = Table('tbl1_survey', dImportParams=d, dParseParams=dParseParams_tbl1_survey)
    tbl.ImportToTblDf(lst_files='tbl1_survey.xlsx')
    return tbl

@pytest.fixture
def row_maj_tbl1_survey(tbl1_survey):
    """
    Instance RowMajorTbl parsing class for survey data
    JDL 9/25/24; Modified 5/30/25
    """
    # Simulate iteration df from .lst_dfs
    tbl1_survey.df_raw = tbl1_survey.lst_dfs[0]

    return parsetables.RowMajorTbl(tbl1_survey)

"""
================================================================================
"""
class TestParseRowMajorTbl1Survey:
    
    def test_survey_ParseDfRawProcedure1(self, row_maj_tbl1_survey):
        """
        Procedure to iteratively parse row major blocks
        (parse a raw table containing two blocks)
        JDL 9/26/24
        """
        row_maj_tbl1_survey.ParseDfRawProcedure()

        #Check that procedure found three blocks
        assert row_maj_tbl1_survey.start_bound_indices == [3, 14, 24]
        assert len(row_maj_tbl1_survey.df) == 11

        df_check = row_maj_tbl1_survey.df

        if IsPrint: print('\n\n', row_maj_tbl1_survey.df, '\n')

        lst_expected = ['Daily', '14.13%', '76', np.nan, np.nan, np.nan]
        check_series_values(df_check.iloc[0], lst_expected)
        lst_expected =  ['Improved cleaning', np.nan, np.nan, '18', '11', '17']
        check_series_values(df_check.iloc[-1], lst_expected)

        if False: print_tables(row_maj_tbl1_survey)

    def xtest_survey_ParseDfRawProcedure2(self, row_maj_tbl1_survey):
        """
        ===Move to ApplyColInfo===
        Procedure to iteratively parse row major blocks
        (parse a raw table containing two blocks)
        Stack the parsed data
        JDL 9/25/24
        """
        row_maj_tbl1_survey.tbl.dParseParams['is_stack_parsed_cols'] = True
        row_maj_tbl1_survey.ParseDfRawProcedure()

        assert len(row_maj_tbl1_survey.df) == 11

        if IsPrint: print('\n\n', row_maj_tbl1_survey.df, '\n')

    def test_survey_ParseBlockProcedure1(self, row_maj_tbl1_survey):
        """
        Parse the survey table and check the final state of the table.
        (1st block)
        JDL 9/25/24
        """
        SetListFirstStartBoundIndex(row_maj_tbl1_survey)
        row_maj_tbl1_survey.ParseBlockProcedure()

        #Check resulting .df relative to tbl1_survey.xlsx
        assert len(row_maj_tbl1_survey.df) == 5

        if IsPrint: print('\n\n', row_maj_tbl1_survey.df, '\n')

        assert list(row_maj_tbl1_survey.df.iloc[0]) == ['Daily', '14.13%', '76']
        assert list(row_maj_tbl1_survey.df.iloc[-1]) == ['Rarely', '0.37%', '2']

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

        if IsPrint: print('\n\n', row_maj_tbl1_survey.df, '\n')

        #Check resulting .df relative to tbl1_survey.xlsx
        assert len(row_maj_tbl1_survey.df) == 3
        assert list(row_maj_tbl1_survey.df.iloc[0]) == ['Lower price point', '91', '33', '19']
        assert list(row_maj_tbl1_survey.df.iloc[-1]) == ['Improved cleaning', '18', '11', '17']

        if False: print_tables(row_maj_tbl1_survey)

    def test_survey_SubsetDataRows(self, row_maj_tbl1_survey):
        """
        Subset rows based on flags and idata_rowoffset_from_flag.
        JDL 9/24/24
        """
        SetListFirstStartBoundIndex(row_maj_tbl1_survey)
        row_maj_tbl1_survey.FindFlagEndBound()
        row_maj_tbl1_survey.ReadHeader()
        row_maj_tbl1_survey.SubsetDataRows()

        # Check resulting .df relative to tbl1_raw.xlsx
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
    dParseParams['parse_type'] = 'RowMajorTbl'
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
    # Instance Table and import data
    d = {'ftype':'excel', 'import_path':files.path_data, 'sht':'raw_table'}
    tbl = Table('tbl1', dImportParams=d, dParseParams=dParseParams_tbl1)
    tbl.ImportToTblDf(lst_files='tbl1_raw.xlsx')
    return tbl

@pytest.fixture
def row_maj_tbl1(tbl1):
    """
    Instance RowMajorTbl parsing class for Table1 data
    (df argument simulates iteration df from .lst_dfs)
    JDL 9/26/24; Modified 5/30/25
    """
    tbl1.df_raw = tbl1.lst_dfs[0] # Simulate iteration through lst_dfs
    parse = parsetables.RowMajorTbl(tbl1)
    return parse

"""
================================================================================
"""
class TestParseRowMajorTbl1Raw:
    """row_major parsing of single block from Excel sheet"""

    def test_ParseDfRawProcedure(self, row_maj_tbl1):
        """
        Procedure to iteratively parse row major blocks
        (parse a raw table containing one block)
        JDL 9/26/24; Modified 4/21/25
        """
        row_maj_tbl1.ParseDfRawProcedure()

        #Check the final state of the table
        self.check_tbl1_values(row_maj_tbl1)

        if IsPrint:
            print_tables(row_maj_tbl1)

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

        assert row_maj_tbl1.df.shape == (5, 4)
        assert row_maj_tbl1.df_raw.shape == (14, 5)
    
    def check_tbl1_values(self, row_maj_tbl1):
        """
        Check the final state of the table (ApplyColInfo sets data types)
        JDL 3/4/24; Modified 4/22/25
        """
        #Check index name and column names 
        assert list(row_maj_tbl1.df.columns) == ['stuff', 'idx_raw', 'col #1', 'col #2' ]

        #Check resulting .tbl.df relative to tbl1_raw.xlsx (import raw to str dtype)
        assert len(row_maj_tbl1.df) == 5
        assert list(row_maj_tbl1.df.loc[0]) == ['Stuff in C', '1', '10', 'a']
        assert list(row_maj_tbl1.df.loc[4]) == ['Stuff in C', '5', '50', 'e']

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
JDL 9/27/24; Refactored 5/30/25
================================================================================
"""
class TestExtractBlockIDs:
    def test_blockids_ExtractBlockIDsProcedure1(self, row_maj_tbl1):
        """
        Procedure to extract block ID values from df_raw
        (Each Block_ID variable input as tuple in tbl.dParseParams['block_id_vars'])
        BlockID's are individual values located relative to first row of data in bloc
        JDL 9/27/24; Updated 5/30/25
        """
        # Run precursor ParseDfRawProcedure and ParseBlockProcedure methods
        self.create_df_block(row_maj_tbl1)
        assert row_maj_tbl1.df_block.shape == (5, 3)

        #Call property to extract block ID(s)
        row_maj_tbl1.df_block = parsetables.RowMajorBlockID(row_maj_tbl1).ExtractBlockIDs

        df_block = row_maj_tbl1.df_block
        assert len(df_block) == 5
        assert list(df_block.columns) == ['stuff', 'idx_raw', 'col #1', 'col #2']
        assert (df_block['stuff'] == 'Stuff in C').all()

    def test_blockids_ExtractBlockIDsProcedure2(self, row_maj_tbl1):
        """
        Procedure to extract block ID values from df_raw
        (Two Block_ID variables input as list)
        JDL 9/27/24
        """
        # Run precursor ParseDfRawProcedure and ParseBlockProcedure methods
        self.create_df_block(row_maj_tbl1)
        assert row_maj_tbl1.df_block.shape == (5, 3)

        #Override default parsing instruction and call property to extract block IDs
        row_maj_tbl1.lst_block_ids = [('stuff', -4, 2), ('stuff2', -2, 1)]
        row_maj_tbl1.df_block = parsetables.RowMajorBlockID(row_maj_tbl1).ExtractBlockIDs

        df_block = row_maj_tbl1.df_block
        assert len(df_block) == 5
        assert list(df_block.columns) == ['stuff', 'stuff2', 'idx_raw', 'col #1', 'col #2']
        assert (df_block['stuff'] == 'Stuff in C').all()
        assert (df_block['stuff2'] == 'flag').all()

    def test_ReadBlockIDValue(self, row_maj_tbl1):
        """
        Set .df_block column from an individual block ID tuple
        JDL 9/27/24; Rewriteen 5/30/25
        """
        # Run precursor ParseDfRawProcedure and ParseBlockProcedure methods
        self.create_df_block(row_maj_tbl1)

        #Instance RowMajorBlockID and get block_id value
        blockid = parsetables.RowMajorBlockID(row_maj_tbl1)

        # use first/only block_id var for testing; check blockid.df_block
        blockid.SetBlockIDColValue(row_maj_tbl1.lst_block_ids[0])
        assert list(blockid.df_block.columns) == ['idx_raw', 'col #1', 'col #2', 'stuff']
        assert (blockid.df_block['stuff'] == 'Stuff in C').all()

    def test_RowMajorBlockID(self, row_maj_tbl1):
        """
        Test initialization with options for .lst_block_ids
        JDL 5/30/25
        """        
        # .lst_block_ids is set as non-list tuple in fixture
        blockid = parsetables.RowMajorBlockID(row_maj_tbl1)
        self.check_blockid_values(blockid)

        # Set to single-item list
        row_maj_tbl1.tbl.dParseParams['block_id_vars'] = [('stuff', -4, 2)]
        blockid = parsetables.RowMajorBlockID(row_maj_tbl1)
        self.check_blockid_values(blockid)

    def check_blockid_values(self, blockid):
        assert isinstance(blockid.lst_block_ids, list)
        assert blockid.lst_block_ids[0][0] == 'stuff'
        assert blockid.lst_block_ids[0][1] == -4
        assert blockid.lst_block_ids[0][2] == 2

    def create_df_block(self, row_maj_tbl1):
        #Precursor methods (ParseDfRawProcedure)
        row_maj_tbl1.AddTrailingBlankRow()
        row_maj_tbl1.SetStartBoundIndices()
        row_maj_tbl1.idx_start_current = row_maj_tbl1.start_bound_indices[0]

        # Precursor methods (ParseBlockProcedure)
        row_maj_tbl1.FindFlagEndBound()
        row_maj_tbl1.ReadHeader()
        row_maj_tbl1.SubsetDataRows()


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

