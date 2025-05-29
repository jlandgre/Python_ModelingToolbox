# Version 4/3/25
# cd Box\ Sync/Projects/Python_Col_Info/tests
import sys, os
import pandas as pd
import numpy as np
import pytest
import datetime as dt

# Add libs folder to sys.path and import project-specific modules
libs_path = os.path.join(os.path.dirname(__file__), '..', 'libs')
sys.path.insert(0, os.path.abspath(libs_path))
from col_info import ColumnInfo
from projfiles import Files
from projtables import ProjectTables
from projtables import Table

IsPrint = False

# files fixture
@pytest.fixture
def files():
    return Files(IsTest=True, subdir_tests='test_data')

@pytest.fixture
def tbls(files):
    tbls = ProjectTables(files)

    #Instance ModelRaw and Model for ColumnInfo testing
    tbls.ModelRaw = Table('ModelRaw')
    tbls.Model = Table('Model')
    return tbls

# Fixture for ColumnInfo - don't initialize df
@pytest.fixture
def cinfo(files):
    return ColumnInfo(files, IsInit=False, IsPrint=False)

# Fixture for ColumnInfo - initialize df by default IsInit=True
@pytest.fixture
def cinfo_init(files):
    return ColumnInfo(files, IsPrint=False)

"""
=========================================================================
InitColInfoProcedure - called by ColumnInfo.__init__()
=========================================================================
"""
class TestInitColInfoProcedure:
    def test_colinfo_init_fixture(self, cinfo_init):
        """
        Test colinfo __init__ with df initialization
        JDL 5/28/25
        """
        assert len(cinfo_init.df) == 17
        assert cinfo_init.df['IsCalculated'].dtype == bool

    def test_ImportColInfoDf(self, cinfo, files):
        """
        Import ColInfo.df from Excel file
        JDL 5/28/25
        """
        cinfo.ImportColInfoDf(files)
        assert len(cinfo.df) == 17

    def test_RecodeColInfoFlagCols(self, cinfo, files):
        """
        Recode ColInfo flag columns to boolean (from imported True/NaN)
        JDL 5/28/25
        """
        cinfo.ImportColInfoDf(files)
        cinfo.RecodeColInfoFlagCols()
        col = cinfo.df['IsCalculated']

        # Check dtype is bool and value counts
        assert col.dtype == bool
        assert (col == False).sum() == 16
        assert col.iloc[16] == True  # Last row is True

    def test_cinfo_fixture(self, cinfo):
        """
        Instance ColumnInfo (typically as cinfo); optionally initialize df
        5/28/25
        """
        assert isinstance(cinfo, ColumnInfo)

"""
=============================================================================
CleanupImportedDataProcedure - Product Sales Example
=============================================================================
"""
@pytest.fixture
def raw_data():
    """
    data for tbls.ModelRaw.df
    JDL 5/22/25
    """
    # Example dataset with three key (index) columns and one value column
    data = {'ABBREV': ['ProdA', 'ProdB', 'ProdA', 'ProdB', 'ProdA', 'ProdA'],
        'DUMMY': 6 * ['xyz'],
        'DATE': 2 * ['2025-04-01', '2025-04-08', '2025-04-15'],
        'RETAILER': 3 * ['WMT'] + 3 * ['TGT'],
        'UNITS': [100., 200., 300., 400., 500., 600.]}
    return pd.DataFrame(data)

class TestColInfoCleanupImported:
    def test_col_info_mockup(self, files):
        """
        Create df_col_info and write to Excel file for testing
        JDL 5/22/25
        """
        data = {'cols':['pl_abbr', None, 'date_wk_start', 'retailer', 'units_redeemed'] + \
                ['date_wk_start', 'pl_abbr', 'units_redeemed_TGT', 'units_redeemed_WMT'], 
            'cols_raw':['ABBREV', 'DUMMY', 'DATE', 'RETAILER', 'UNITS'] + 4 * [None], 
            'idx_order':[2, None, 1, 3, None] + [1, 2, None, None], 
            'cols_order':[2, None, 1, 3, 4] + [1, 2, 3, 4],
            'data_type':['str', None, 'dt.date', 'str', 'float'] + \
                ['dt.date', 'str', 'float', 'float'], 
            'tbl_name':5 * ['ModelRaw'] + 4 * ['Model'],}
        df_col_info = pd.DataFrame(data)

        assert df_col_info.index.size == 9

        fil = df_col_info['tbl_name']=='ModelRaw'
        assert df_col_info[fil].index.size == 5
        fil = df_col_info['tbl_name']=='Model'
        assert df_col_info[fil].index.size == 4

        # Write to test_data to allow realistic full testing of col_info
        #df_col_info.to_excel(files.pf_col_info, index=False, sheet_name='cols')

    def test_raw_data_fixture(self, raw_data):
        """
        Check raw_data df
        JDL 5/22/25
        """
        assert isinstance(raw_data, pd.DataFrame)
        assert raw_data.shape == (6, 5)

        if IsPrint: 
            print('\n\n', raw_data)

    def test_RenameColsRawData(self, raw_data, cinfo_init, tbls):
        """
        """
        tbls.ModelRaw.df = raw_data
        cinfo.RenameColsRawData(tbls.ModelRaw)
        lst = ['pl_abbr', 'DUMMY', 'date_wk_start', 'retailer', 'units_redeemed']
        assert tbls.ModelRaw.df.columns.tolist() == lst

        if IsPrint:
            print('\n\n', tbls.ModelRaw.df)

    def test_SetImportedKeepCols(self, raw_data, cinfo, tbls):
        """
        """
        tbls.ModelRaw.df = raw_data
        cinfo.RenameColsRawData(tbls.ModelRaw)
        cinfo.SetImportedKeepCols(tbls.ModelRaw)
        
        lst = ['date_wk_start', 'pl_abbr', 'retailer', 'units_redeemed']
        assert tbls.ModelRaw.df.columns.tolist() == lst

        if IsPrint:
            print('\n\n', tbls.ModelRaw.df.info())

    def test_SetTblDataTypes(self, raw_data, cinfo, tbls):
        """
        Set data types for tbl.df columns based on cinfo.df metadata
        5/22/25
        """
        tbls.ModelRaw.df = raw_data
        cinfo.RenameColsRawData(tbls.ModelRaw)
        cinfo.SetImportedKeepCols(tbls.ModelRaw)
        cinfo.SetTblDataTypes(tbls.ModelRaw)

        # Check that date_wk_start and units_redeemed data types
        assert tbls.ModelRaw.df['date_wk_start'].apply(lambda x: isinstance(x, dt.date)).all()
        assert tbls.ModelRaw.df['units_redeemed'].apply(lambda x: isinstance(x, float)).all()
"""
=============================================================================
Class ColumnInfo - DataIngestionProcedure
=============================================================================
"""
def test_DataIngestionProcedure(tbls, cinfo):
    """
    Procedure to import Excel data, subset to keep_cols_import and
    replace import names
    JDL 4/3/25
    """
    col_info.DataIngestionProcedure(tbls)

    print('\n\n', tbls.ExampleTbl1.df)
    print('\n', tbls.ExampleTbl2.df, '\n')

def test_ReplaceImportNames1(col_info, tbls):
    """
    Replace import names for ExampleTbl1
    JDL 4/3/25
    """
    # Precursor methods
    tbls.ImportExcelInputs()
    col_info.SetFlagColsBoolean(tbls)
    col_info.SetTblKeepColsFromImport(tbls, 'ExampleTbl1')

    # Call ReplaceImportNames
    col_info.ReplaceImportNames(tbls, 'ExampleTbl1')

    # Check that the column names are correctly replaced
    expected = ['date1', 'col_1a', 'col_1b']
    assert list(tbls.ExampleTbl1.df.columns) == expected

def test_ReplaceImportNames2(col_info, tbls):
    """
    Replace import names for ExampleTbl2
    JDL 4/3/25
    """
    # Precursor methods
    tbls.ImportExcelInputs()
    col_info.SetFlagColsBoolean(tbls)
    col_info.SetTblKeepColsFromImport(tbls, 'ExampleTbl2')

    # Call ReplaceImportNames
    col_info.ReplaceImportNames(tbls, 'ExampleTbl2')

    # Check that the column names are correctly replaced
    expected = ['date2', 'col_2a', 'col_2c']
    assert list(tbls.ExampleTbl2.df.columns) == expected

def test_SetTblKeepColsFromImport(col_info, tbls):
    """
    Subset tbl.df columns based on cinfo.df
    JDL 4/3/25
    """
    # Precursor methods
    tbls.ImportExcelInputs()
    col_info.SetFlagColsBoolean(tbls)

    # Subset columns for ExampleTbl1 (no deletions)
    col_info.SetTblKeepColsFromImport(tbls, 'ExampleTbl1')
    expected = ['date1_import_name', 'col_1a_import_name', 'col_1b_import_name']
    assert list(tbls.ExampleTbl1.df.columns) == expected

    # Subset columns for ExampleTbl2 (col_dummy gets deleted)
    col_info.SetTblKeepColsFromImport(tbls, 'ExampleTbl2')
    expected = ['date2_import_name', 'col_2a_import_name', 'col_2c_import_name']
    assert list(tbls.ExampleTbl2.df.columns) == expected

def test_tbls_ImportExcelInputs(tbls):
    """
    Import ExampleTbl1 and ExampleTbl2
    JDL 4/3/25
    """
    tbls.ImportExcelInputs()
    assert len(tbls.ExampleTbl1.df) == 3
    assert len(tbls.ExampleTbl2.df) == 6

def test_SetFlagColsBoolean(cinfo, tbls):
    """
    Fill False for NaN values in tblCI flag columns 
    (allow True/<blank> entries)
    JDL 4/3/25; Updated 5/28/25
    """
    cinfo.SetFlagColsBoolean(tbls)

    # Check that IsCalculated column is converted to T/F Boolean
    expected = 16 * [False] + [True]
    assert list(tbls.ColInfo.df['IsCalculated']) == expected

    expected = [True, True, True, True, False, True, True, False]
    assert list(tbls.ColInfo.df['keep_col_import']) == expected

def test_tbls_fixture(tbls):
    """
    Test - Check tbls.ColInfo instanced and filename correct
    JDL 4/3/25; modified 5/28/25
    """
    # Check that ColInfo initialized
    assert hasattr(tbls, 'files')
    assert tbls.files.pf_col_info.split(os.sep)[-1] == 'col_info.xlsx'

def test_files_fixture(files):
    """
    Test - Check that last two items in files paths are 'libs' and 'col_info.xlsx'
    JDL 4/3/25
    """
    # trailing os.sep creates '' final list item
    lst = files.path_root.split(os.sep)
    assert lst[-1] == ''
    root_folder = lst[-2]

    # Check libs, tests paths relative to root 
    lst = files.path_libs.split(os.sep)
    assert lst[-1] == ''
    assert lst[-2] == 'libs'
    assert lst[-3] == root_folder

    lst = files.path_tests.split(os.sep)
    assert lst[-1] == ''
    assert lst[-2] == 'tests'
    assert lst[-3] == root_folder

    lst = files.pf_col_info.split(os.sep)
    assert lst[-1] == 'col_info.xlsx'
    assert lst[-2] == 'test_data'