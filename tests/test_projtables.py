# Version 4/21/25
# cd Box\ Sync/Projects/Python_Col_Info/tests
import sys, os
import pandas as pd
import numpy as np
import pytest

# Add libs folder to sys.path and import project-specific modules
libs_path = os.path.join(os.path.dirname(__file__), '..', 'libs')
sys.path.insert(0, os.path.abspath(libs_path))
from projfiles import Files
from projtables import ProjectTables
from projtables import Table
from col_info import ColumnInfo

IsPrint = True

@pytest.fixture
def files():
    return Files(IsTest=True, subdir_tests='test_data')

@pytest.fixture
def tbls(files):
    return ProjectTables(files)

@pytest.fixture
def tbls_ExcelFile(files):
    """ Like tbls.ExcelFile"""
    d = {'ftype':'excel', 'import_path':files.path_data}
    return Table('ExcelFile', dImportParams=d)

@pytest.fixture
def tbls_CSVFile(files):
    """ Like tbls.CSVFile"""
    d = {'ftype':'csv', 'import_path':files.path_data}
    return Table('CSVFile', dImportParams=d)

"""
================================================================================
Table.__init__()
JDL 5/28/25
================================================================================
"""
class TestTableInit:
    def test_Table_init1(self, files):
        """
        Initialize Table without col_info
        JDL 5/28/25
        """
        # Instance Table with all default parameters
        tbl = Table('TestTable')
        assert tbl.name == 'TestTable'
        assert tbl.dfColInfo is None

    def test_Table_init2(self, files):
        """
        Initialize Table with col_info
        JDL 5/28/25
        """
        # Instance ColumnInfo containing mockup metadata for multiple tables
        cinfo = ColumnInfo(files, IsInit=True, IsPrint=False)
        assert len(cinfo.df) == 17

        # Instance ModelRaw with col_info
        tbl = Table('ModelRaw', col_info=cinfo)
        assert tbl.name == 'ModelRaw'
        assert type(tbl.dfColInfo) == pd.DataFrame
        assert len(tbl.dfColInfo) == 5

        if IsPrint: print('\n', tbl.dfColInfo)

"""
================================================================================
ImportToTblDf Procedure
JDL 4/10/25 Rewritten to allow multisheet Excel and separate ingest/parse
================================================================================
"""
"""
importing Excel files
"""

def test_ImportToTblDf1(tbls_ExcelFile):
    """
    Import unstructured file+sht that needs parsing
    JDL 4/10/25
    """
    # Fixture sets ftype and import_path
    tbls_ExcelFile.dImportParams.update({'lst_files':'tbl1_raw.xlsx'})
    tbls_ExcelFile.dParseParams.update({'is_unstructured':True})

    tbls_ExcelFile.ImportToTblDf()

    # Import should leave unparsed df in lst_dfs for later parsing
    assert tbls_ExcelFile.df.empty
    assert len(tbls_ExcelFile.lst_dfs) == 1
    assert tbls_ExcelFile.lst_dfs[0].iloc[2, 1] == 'Stuff'

    if IsPrint: print('\n', tbls_ExcelFile.lst_dfs[0])
    if IsPrint: print('\n', tbls_ExcelFile.lst_dfs[0].info())

def test_ImportToTblDf2(tbls_ExcelFile):
    """
    Import Excel rows/cols table to .df
    4/8/25
    """
    # Fixture sets ftype and import_path; default is structured
    tbls_ExcelFile.dImportParams['lst_files'] = 'Example2.xlsx'
    tbls_ExcelFile.ImportToTblDf()

    # Import should result in .df with 6 rows (no parsing needed)
    check_ExcelFile(tbls_ExcelFile)

    if IsPrint: print('\n', tbls_ExcelFile.df)

def test_ImportToTblDf3(tbls_ExcelFile):
    """
    Import Excel structured table with skip_rows
    4/8/25
    """
    # Fixture sets ftype and import_path; default is structured
    tbls_ExcelFile.dImportParams.update({'lst_files':'Example2_skiprows.xlsx'})
    tbls_ExcelFile.dParseParams.update({'n_skip_rows':2})
    tbls_ExcelFile.ImportToTblDf()

    # Import should result in .df with 6 rows (no parsing needed)
    check_ExcelFile(tbls_ExcelFile)

def test_ImportToTblDf4(tbls_ExcelFile):
    """
    .df from Excel structured tables on one sheet in multiple files
    JDL 4/10/25
    """
    # Fixture sets ftype and import_path; default is structured
    f_lst = ['Example2a.xlsx', 'Example2b.xlsx']
    tbls_ExcelFile.dImportParams.update({'lst_files':f_lst,
        'sht_type':'all', 'sht':'data'})
    tbls_ExcelFile.ImportToTblDf()

    # Import should result in .df with 6 rows (no parsing needed)
    check_ExcelFile(tbls_ExcelFile)

def test_ImportToTblDf5(tbls_ExcelFile):
    """
    .df from Excel structured tables on one, specified sheet
    JDL 4/14/25
    """
    # Fixture sets ftype and import_path; default is structured
    tbls_ExcelFile.dImportParams.update({'lst_files':'Example2.xlsx',
        'sht_type':'single', 'sht':'data'})
    tbls_ExcelFile.ImportToTblDf()

    # Import should result in .df with 6 rows (no parsing needed)
    check_ExcelFile(tbls_ExcelFile)

def check_ExcelFile(tbls_ExcelFile):
    """
    Helper function to check ExcelFile import
    JDL 4/8/25
    """
    assert isinstance(tbls_ExcelFile.df, pd.DataFrame)
    assert len(tbls_ExcelFile.df) == 6

def test_Instance_ExcelFile_Table(tbls):
    """
    Test - Initialize tbls.ExcelFile as an instance of Table
    (e.g. example of on-the-fly tbls Table creation)
    JDL 4/8/25
    """
    # Initialize tbls.ExcelFile as an instance of Table
    tbls.ExcelFile = Table('ExcelFile')

    # Assert that tbls.ExcelFile is correctly initialized
    assert isinstance(tbls.ExcelFile, Table)
    assert tbls.ExcelFile.name == 'ExcelFile'

"""
importing CSV files
"""
def test_ImportToTblDf_CSV1(tbls_CSVFile):
    """
    Import CSV rows/cols table to tbls.CSVFile.df_raw
    4/9/25
    """
    tbls_CSVFile.dImportParams.update({'lst_files':'Example2.csv'})
    tbls_CSVFile.ImportToTblDf()

    # Import should result in .df with 6 rows (no parsing needed)
    check_CSVFile(tbls_CSVFile)

def test_ImportToTblDf_CSV2(tbls_CSVFile):
    """
    Import CSV structured table with skip_rows
    4/10/25
    """
    tbls_CSVFile.dImportParams.update({'lst_files':'Example2_skiprows.csv'})
    tbls_CSVFile.dParseParams.update({'n_skip_rows':2})
    tbls_CSVFile.ImportToTblDf()

    # Import should result in .df with 6 rows (no parsing needed)
    check_CSVFile(tbls_CSVFile)

def test_ImportToTblDf_CSV3(tbls_CSVFile):
    """
    Import CSV structured table from multiple raw files
    4/10/25
    """
    f_lst = ['Example2a.csv', 'Example2b.csv']
    tbls_CSVFile.dImportParams.update({'lst_files':f_lst})
    tbls_CSVFile.ImportToTblDf()

    # Import should result in .df with 6 rows (no parsing needed)
    check_CSVFile(tbls_CSVFile)

def test_ImportToTblDf_CSV4(tbls_CSVFile):
    """
    Import CSV unstructured table to .lst_dfs
    JDL 4/10/25
    """
    tbls_CSVFile.dImportParams.update({'lst_files':'tbl1_raw.csv'})
    tbls_CSVFile.dParseParams.update({'is_unstructured':True})
    tbls_CSVFile.ImportToTblDf()
    
    if IsPrint: print('\n', tbls_CSVFile.lst_dfs[0])

    # Import should leave unparsed df in lst_dfs for later parsing
    assert tbls_CSVFile.df.empty
    assert len(tbls_CSVFile.lst_dfs) == 1
    assert tbls_CSVFile.lst_dfs[0].iloc[2, 1] == 'Stuff'


def check_CSVFile(tbls_CSVFile):
    """
    Helper function to check CSVFile import
    4/9/25
    """
    assert isinstance(tbls_CSVFile.df, pd.DataFrame)
    assert len(tbls_CSVFile.df) == 6

"""
Tests of ImportToTblDf procedure methods
"""
def test_ImportToTblDf_SetLstFiles():
    """
    Test .SetLstFiles method with various input scenarios.
    if lst_files arg specified, that overrides consideration of
    dImportParams['lst_files']
    if 'import_path' in dImportParams, it is prepended to each file name
    """

    # Case 1: lst_files = None; dImportParams['lst_files'] = 'Example1.xlsx'
    tbl = Table('ExcelTable')
    tbl.dImportParams = {'lst_files':'Example1.xlsx'}
    lst_files = tbl.SetLstFiles(None)
    assert lst_files == ['Example1.xlsx']

    # Case 2: lst_files = None; dImportParams['lst_files'] = ['Example2.xlsx']
    tbl = Table('ExcelTable')
    tbl.dImportParams = {'lst_files':['Example2.xlsx']}
    lst_files = tbl.SetLstFiles(None)
    assert lst_files == ['Example2.xlsx']

    # Case 3: lst_files = ['Example3.xlsx']
    tbl = Table('ExcelTable')
    tbl.dImportParams = {}
    lst_files = tbl.SetLstFiles(['Example3.xlsx'])
    assert lst_files == ['Example3.xlsx']

    # Case 4: 'import_path' = "TestPath/" in dImportParams 
    tbl = Table('ExcelTable')
    tbl.dImportParams = {'lst_files':'Example4.xlsx', 'import_path':'TestPath/'}
    lst_files = tbl.SetLstFiles(None)
    assert lst_files == ['TestPath/Example4.xlsx']

    # Case 5: 'import_path' with multiple files
    tbl = Table('ExcelTable')
    tbl.dImportParams = {'lst_files':['Example5a.xlsx', 'Example5b.xlsx'], 
                    'import_path':'TestPath/'}
    lst_files = tbl.SetLstFiles(None)
    assert lst_files == ['TestPath/Example5a.xlsx', 'TestPath/Example5b.xlsx']

def test_ImportToTblDf_SetFileIngestParams():
    """
    Set temporary Table attributes for the current file
    JDL 4/10/25
    """
    # All defaults (sht not specified)
    tbl = Table('ExcelFile', dImportParams={'ftype':'excel'})
    tbl.SetFileIngestParams()
    assert tbl.is_unstructured == False
    assert tbl.n_skip_rows == 0
    assert tbl.parse_type == 'none'
    assert tbl.sht_type == 'single'

    # Specified with sht_type='all'
    d1 = {'ftype':'excel', 'sht_type':'all'}
    d2 = {'is_unstructured':True, 'n_skip_rows':2, 
                        'parse_type':'row_major'}
    tbl = Table('ExcelFile', dImportParams=d1, dParseParams=d2)
    tbl.SetFileIngestParams()
    assert tbl.is_unstructured == True
    assert tbl.n_skip_rows == 2
    assert tbl.parse_type == 'row_major'
    assert tbl.sht_type == 'all'

def test_ImportToTblDf_Excel_SetLstSheets1(files):
    """
    Set .lst_sheets with single, specified sheet name
    JDL 4/10/25
    """
    tbl = Table('ExcelFile', dImportParams={'ftype':'excel', 
                                'import_path':files.path_data, 
                                'sht_type':'single', 'sht':'data'})
    tbl.SetFileIngestParams()
    tbl.pf = tbl.dImportParams['import_path'] + 'Example2.xlsx'
    tbl.SetLstSheets()
    assert tbl.lst_sheets == ['data']

def test_ImportToTblDf_Excel_SetLstSheets2(files):
    """
    Set .lst_sheets with sht_type='all' to specify all sheets in file
    JDL 4/10/25
    """
    tbl = Table('ExcelFile', dImportParams={'ftype':'excel', 
                                    'import_path':files.path_data, 
                                    'sht_type':'all'})
    tbl.SetFileIngestParams()
    tbl.pf = tbl.dImportParams['import_path'] + 'Example2_multisheet.xlsx'
    tbl.SetLstSheets()
    assert tbl.lst_sheets == ['data1', 'data2']

def test_ImportToTblDf_Excel_SetLstSheets3(files):
    """
    Set .lst_sheets 'single' but no specified sheet name
    (defaults to first sheet --should be 'data' in saved test file)
    JDL 4/10/25
    """
    tbl = Table('ExcelFile', dImportParams={'ftype':'excel', 
                                'import_path':files.path_data, 
                                'sht_type':'single'})
    tbl.SetFileIngestParams()
    tbl.pf = tbl.dImportParams['import_path'] + 'Example2.xlsx'
    tbl.SetLstSheets()
    assert tbl.lst_sheets == ['data']

def test_ImportToTblDf_Excel_ReadExcelSht1(files):
    """
    Read from structured Excel sheet into a temporary DataFrame
    JDL 4/10/25
    """
    dImportParams={'ftype':'excel', 'sht_type':'single', 'sht':'data'}
    dParseParams={'is_unstructured':False}
    tbl = Table('ExcelFile', dImportParams=dImportParams,
        dParseParams=dParseParams)

    # .ReadExcelSht uses .is_unstructured, .pf, .sht and .n_skip_rows
    tbl.pf = files.path_data + 'Example2.xlsx'
    tbl.SetFileIngestParams()
    tbl.SetLstSheets() # Not used but included for predecessor completeness
    assert tbl.lst_sheets == ['data']

    # tbl.sht is loop control for iterating over file's sheets per sht_type
    tbl.sht = tbl.lst_sheets[0]
    tbl.ReadExcelSht()
    assert len(tbl.df_temp) == 6

def test_ImportToTblDf_Excel_ReadExcelSht2(files):
    """
    Read from structured Excel sheet into a temporary DataFrame
    (with skip_rows)
    JDL 4/10/25
    """
    dImportParams={'ftype':'excel', 'sht_type':'single', 'sht':'data'}
    dParseParams={'is_unstructured':False, 'n_skip_rows':2}
    tbl = Table('ExcelFile', dImportParams=dImportParams,
        dParseParams=dParseParams)

    # .ReadExcelSht uses .is_unstructured, .pf, .sht and .n_skip_rows
    tbl.pf = files.path_data + 'Example2_skiprows.xlsx'
    tbl.SetFileIngestParams()
    tbl.SetLstSheets() # Not used but included for predecessor completeness
    assert tbl.lst_sheets == ['data']

    # tbl.sht is loop control for iterating over file's sheets per sht_type
    tbl.sht = tbl.lst_sheets[0]
    tbl.ReadExcelSht()
    assert len(tbl.df_temp) == 6

def test_ImportToTblDf_Excel_ReadExcelSht3(files):
    """
    Read from unstructured Excel sheet into a temporary DataFrame
    JDL 4/10/25
    """
    dImportParams={'ftype':'excel', 'sht_type':'single', 'sht':'raw_table'}
    dParseParams={'is_unstructured':True}
    tbl = Table('ExcelFile', dImportParams=dImportParams,
        dParseParams=dParseParams)

    # .ReadExcelSht uses .is_unstructured, .pf, .sht and .n_skip_rows
    tbl.pf = files.path_data + 'tbl1_raw.xlsx'
    tbl.SetFileIngestParams()
    tbl.SetLstSheets() # Not used but included for predecessor completeness
    assert tbl.lst_sheets == ['raw_table']

    # tbl.sht is loop control for iterating over file's sheets per sht_type
    tbl.sht = tbl.lst_sheets[0]
    tbl.ReadExcelSht()
    assert tbl.df_temp.iloc[2, 1] == 'Stuff'

def test_ImportToTblDf_Excel_ReadExcelFileSheets1(files):
    """
    Loop through sheets in lst_sheets and read their data
    (multiple sheets from same file)
    JDL 4/10/25
    """
    dImportParams={'ftype':'excel', 'sht_type':'all', 
                                'lst_files':['Example2_multisheet.xlsx']}
    tbl = Table('ExcelFile', dImportParams=dImportParams)

    # Predecessors are mockup of ImportToTblDf steps
    tbl.lst_dfs = []

    # tbl.df is loop variable in ImportToTblDf
    tbl.pf = files.path_data + tbl.dImportParams['lst_files'][0]

    tbl.SetFileIngestParams()
    tbl.SetLstSheets()
    assert tbl.lst_sheets == ['data1', 'data2']

    tbl.ReadExcelFileSheets()
    assert len(tbl.lst_dfs) == 2
    assert tbl.lst_dfs[0].shape == (2, 4)
    assert tbl.lst_dfs[1].shape == (4, 4)

def test_ImportToTblDf_Excel_ReadExcelFileSheets2(files):
    """
    Loop through sheets in lst_sheets and read their data
    (single sheet from a file)
    JDL 4/10/25
    """
    dImportParams={'ftype':'excel', 'sht_type':'single', 'sht':'data',
                                'lst_files':['Example2.xlsx']}
    tbl = Table('ExcelFile', dImportParams=dImportParams)

    # Predecessors lines are mockup of ImportToTblDf steps
    tbl.lst_dfs = []

    # tbl.df is loop variable in ImportToTblDf
    tbl.pf = files.path_data + tbl.dImportParams['lst_files'][0]

    tbl.SetFileIngestParams()
    tbl.SetLstSheets()
    tbl.ReadExcelFileSheets()
    assert len(tbl.lst_dfs) == 1
    assert tbl.lst_dfs[0].shape == (6, 4)

"""
Tests of fixtures and utilities
"""

def test_tbls_fixture(files, tbls):
    """
    Test - tbls fixture
    4/8/25; updated 5/29/25
    """
    # Check that tbls is an instance of ProjectTables
    assert isinstance(files, Files)
    assert isinstance(tbls, ProjectTables)

