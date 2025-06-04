# Version 6/4/25
# cd Box\ Sync/Projects/Python_Modeling_Toolbox/tests
import sys, os
import pandas as pd
import numpy as np
import pytest
import datetime as dt
from datetime import datetime

# Add libs folder to sys.path and import project-specific modules
libs_path = os.path.join(os.path.dirname(__file__), '..', 'libs')
sys.path.insert(0, os.path.abspath(libs_path))
from col_info import ColumnInfo
from projfiles import Files
from projtables import ProjectTables
from projtables import Table

IsPrint = True

# files fixture
@pytest.fixture
def files():
    return Files(IsTest=True, subdir_tests='test_data')

@pytest.fixture
def tbls(files):
    return ProjectTables(files)

# Fixture for ColumnInfo - don't initialize df
@pytest.fixture
def cinfo_no_init(files):
    return ColumnInfo(files, IsInit=False, IsPrint=False)

# Fixture for ColumnInfo - initialize df by default IsInit=True
@pytest.fixture
def cinfo(files):
    return ColumnInfo(files, IsInit=True, IsPrint=False)

"""
=========================================================================
InitColInfoProcedure - called by ColumnInfo.__init__()
=========================================================================
"""
class TestInitColInfoProcedure:
    def test_colinfo_init_fixture(self, cinfo):
        """
        Test colinfo __init__ with df initialization
        JDL 5/28/25
        """
        assert len(cinfo.df) == 17
        assert cinfo.df['IsCalculated'].dtype == bool

    def test_ImportColInfoDf(self, cinfo_no_init, files):
        """
        Import ColInfo.df from Excel file
        JDL 5/28/25
        """
        cinfo_no_init.ImportColInfoDf(files)
        assert len(cinfo_no_init.df) == 17

    def test_RecodeColInfoFlagCols(self, cinfo_no_init, files):
        """
        Recode ColInfo flag columns to boolean (from imported True/NaN)
        JDL 5/28/25
        """
        cinfo_no_init.ImportColInfoDf(files)
        cinfo_no_init.RecodeColInfoFlagCols()
        col = cinfo_no_init.df['IsCalculated']

        # Check dtype is bool and value counts
        assert col.dtype == bool
        assert len(cinfo_no_init.df) == 17
        assert (col == False).sum() == 16
        assert col.iloc[16] == True  # Last row is True

    def test_cinfo_no_init_fixture(self, cinfo_no_init):
        """
        Instance ColumnInfo (typically as cinfo); optionally initialize df
        5/28/25
        """
        assert isinstance(cinfo_no_init, ColumnInfo)

"""
=============================================================================
CleanupImportedDataProcedure - Product Sales Example
This example does not enable tbls' optional UseColInfo flag. It instances  
col_info as a separate object to make validation simpler. It instances
individual tables with col_info parameter which populates their df.col_info
table-specific metadata about their variables.
JDL 5/28/25
=============================================================================
"""
@pytest.fixture
def tbls1(files, cinfo):
    """
    cinfo fixture includes column info for ModelRaw
    JDL 5/28/25
    """
    tbls1 = ProjectTables(files, UseColInfo=True)
    tbls1.ModelRaw = Table('ModelRaw', col_info=tbls1.col_info)

    # Example dataset with three key (index) columns and one value column
    # All strings as if imported with Table.ImportToTblDf()
    data = {'ABBREV': ['ProdA', 'ProdB', 'ProdA', 'ProdB', 'ProdA', 'ProdA'],
        'DUMMY': 6 * ['xyz'],
        'DATE': 2 * ['2025-04-01', '2025-04-08', '2025-04-15'],
        'RETAILER': 3 * ['WMT'] + 3 * ['TGT'],
        'units_redeemed': ['100.', '200.', '300.', '400.', '500.', '600.']}
    tbls1.ModelRaw.df = pd.DataFrame(data)
    return tbls1

"""
=========================================================================
Other Methods
=========================================================================
"""
class TestOtherMethods:
    def test_SetTblIndexList(self, tbls1):
        """
        Set tbl's .idx attribute to a list of index columns
        (test_tbls1_fixture checks that tbls.ModelRaw.dfColInfo is set)
        JDL 6/4/25
        """
        tbls1.col_info.SetTblIndexList(tbls1.ModelRaw)
        assert tbls1.ModelRaw.idx == ['date_wk_start', 'pl_abbr', 'retailer']


class TestColInfoCleanupImportedSales:
    def test_CleanupImportedDataProcedure1(self, cinfo_no_init, tbls1):
        """
        Overall Procedure to subset/reorder imported columns and set data types 
        JDL 5/28/25
        """
        if IsPrint:
            print('\n\nInitial - Before Cleanup\n\n', tbls1.ModelRaw.df, '\n\n')
            print(tbls1.ModelRaw.df.info())

        cinfo_no_init.CleanupImportedDataProcedure(tbls1.ModelRaw)

        lst = ['date_wk_start', 'pl_abbr', 'retailer', 'units_redeemed']
        assert tbls1.ModelRaw.df.columns.tolist() == lst
        for col, type_ in zip(['date_wk_start', 'units_redeemed'], [dt.date, float]):
            assert tbls1.ModelRaw.df[col].apply(lambda x: isinstance(x, type_)).all()

        if IsPrint:
            print('\nFinal - After cleanup\n\n', tbls1.ModelRaw.df, '\n\n')
            print(tbls1.ModelRaw.df.info())
            print('\n\n')

    def test_RenameColsRawData(self, cinfo_no_init, tbls1):
        """
        Rename raw data columns post-import
        JDL 5/28/25
        """
        cinfo_no_init.RenameColsRawData(tbls1.ModelRaw)
        lst = ['pl_abbr', 'DUMMY', 'date_wk_start', 'retailer', 'units_redeemed']
        assert tbls1.ModelRaw.df.columns.tolist() == lst

    def test_SetImportedKeepCols(self, cinfo_no_init, tbls1):
        """
        Subset imported keep_cols for tbl
        JDL 5/22/25
        """
        cinfo_no_init.RenameColsRawData(tbls1.ModelRaw)
        cinfo_no_init.SetImportedKeepCols(tbls1.ModelRaw)
        
        lst = ['date_wk_start', 'pl_abbr', 'retailer', 'units_redeemed']
        assert tbls1.ModelRaw.df.columns.tolist() == lst

    def test_SetTblDataTypes(self, cinfo_no_init, tbls1):
        """
        Set data types for tbl.df columns based on cinfo.df metadata
        5/22/25; updated 5/28/25
        """
        cinfo_no_init.RenameColsRawData(tbls1.ModelRaw)
        cinfo_no_init.SetImportedKeepCols(tbls1.ModelRaw)
        cinfo_no_init.SetTblDataTypes(tbls1.ModelRaw)

        # Check date_wk_start and units_redeemed data types for all values
        assert tbls1.ModelRaw.df['date_wk_start'].apply(lambda x: isinstance(x, dt.date)).all()
        assert tbls1.ModelRaw.df['units_redeemed'].apply(lambda x: isinstance(x, float)).all()

    def test_tbls1_fixture(self, files, tbls1):
        """
        Check Raw Data
        JDL 5/28/25
        """
        assert isinstance(tbls1.ModelRaw.df, pd.DataFrame)
        assert tbls1.ModelRaw.df.shape == (6, 5)
        lst = ['ABBREV', 'DUMMY', 'DATE', 'RETAILER', 'units_redeemed']
        assert tbls1.ModelRaw.df.columns.tolist() == lst

        # Check tbls.col_info.df got created
        assert tbls1.col_info.df.shape[1] == 9
        assert tbls1.col_info.df.shape[0] >= 5

        # Check individual table's dfColInfo got created
        assert isinstance(tbls1.ModelRaw.dfColInfo, pd.DataFrame)
        assert tbls1.ModelRaw.dfColInfo.shape == (5, 9)

"""
=============================================================================
CleanupImportedDataProcedure - ExampleTbl1 and ExampleTbl2 without tbl_info
=============================================================================
"""
@pytest.fixture
def tbls2(files, cinfo):
    """
    cinfo fixture includes column info for ExampleTbl1 and ExampleTbl2
    JDL 5/28/25
    """
    # Instance ProjectTables
    tbls2 = ProjectTables(files)
    
    # Define import parameters (same for both tables)
    d = {'import_path':files.path_data, 'ftype': 'excel', 'sht':'data'}

    # Instance and import tables (to tbls2.Example1.df and .Example2.df)
    tbls2.ExampleTbl1 = Table('ExampleTbl1', dImportParams=d, col_info=cinfo)
    tbls2.ExampleTbl1.ImportToTblDf(lst_files='Example1.xlsx') 

    tbls2.ExampleTbl2 = Table('ExampleTbl2', dImportParams=d, col_info=cinfo)
    tbls2.ExampleTbl2.ImportToTblDf(lst_files='Example2.xlsx') 
    return tbls2

class TestColInfoCleanupImportedExamples:
    def x_test_CleanupImportedDataProcedure2(self, cinfo, tbls2):
        """
        Overall Procedure to subset/reorder imported columns and set data types 
        JDL 5/28/25
        """
        if IsPrint:
            print('\n\nInitial - Before Cleanup\n\n', tbls2.ExampleTbl1.df, '\n\n')
            print(tbls2.ExampleTbl1.df.info())
            print('\n\nInitial - Before Cleanup\n\n', tbls2.ExampleTbl2.df, '\n\n')
            print(tbls2.ExampleTbl2.df.info())
            pr

        cinfo.CleanupImportedDataProcedure(tbls2.ModelRaw)

        lst = ['date_wk_start', 'pl_abbr', 'retailer', 'units_redeemed']
        assert tbls1.ModelRaw.df.columns.tolist() == lst
        for col, type_ in zip(['date_wk_start', 'units_redeemed'], [dt.date, float]):
            assert tbls1.ModelRaw.df[col].apply(lambda x: isinstance(x, type_)).all()

        if IsPrint:
            print('\nFinal - After cleanup\n\n', tbls1.ModelRaw.df, '\n\n')
            print(tbls1.ModelRaw.df.info())
            print('\n\n')
            
    def test_RenameColsRawData2(self, cinfo, tbls2):
        """
        Rename raw data columns post-import
        JDL 5/28/25
        """
        cinfo.RenameColsRawData(tbls2.ExampleTbl1)
        cinfo.RenameColsRawData(tbls2.ExampleTbl2)
        lst = ['date1', 'col_1a', 'col_1b']
        assert tbls2.ExampleTbl1.df.columns.tolist() == lst
        lst = ['date2', 'col_dummy', 'col_2a', 'col_2c']
        assert tbls2.ExampleTbl2.df.columns.tolist() == lst

    def test_SetImportedKeepCols2(self, cinfo, tbls2):
        """
        Subset imported keep_cols for tbl
        JDL 5/28/25
        """
        for tbl in [tbls2.ExampleTbl1, tbls2.ExampleTbl2]:
            cinfo.RenameColsRawData(tbl)
            cinfo.SetImportedKeepCols(tbl)
        
        lst = ['date1', 'col_1a', 'col_1b']
        assert tbls2.ExampleTbl1.df.columns.tolist() == lst
        lst = ['date2', 'col_2a', 'col_2c']
        assert tbls2.ExampleTbl2.df.columns.tolist() == lst

    def test_SetTblDataTypes2(self, cinfo, tbls2):
        """
        Set data types for tbl.df columns based on cinfo.df metadata
        5/22/25; updated 5/28/25
        """
        for tbl in [tbls2.ExampleTbl1, tbls2.ExampleTbl2]:
            cinfo.RenameColsRawData(tbl)
            cinfo.SetImportedKeepCols(tbl)
            cinfo.SetTblDataTypes(tbl)

        # Check date_wk_start and units_redeemed data types for all values
        assert tbls2.ExampleTbl1.df['date1'].apply(lambda x: isinstance(x, dt.date)).all()
        assert tbls2.ExampleTbl2.df['date2'].apply(lambda x: isinstance(x, pd.Timestamp)).all()

    def test_tbls2_fixture(self, tbls2):
        """
        Check Imported Data
        JDL 5/28/25
        """
        # Check type and shape
        assert isinstance(tbls2.ExampleTbl1.df, pd.DataFrame)
        assert isinstance(tbls2.ExampleTbl2.df, pd.DataFrame)
        assert tbls2.ExampleTbl1.df.shape == (3, 3)
        assert tbls2.ExampleTbl2.df.shape == (6, 4)

        # check columns are as expected
        lst = ['date1_import_name', 'col_1a_import_name', 'col_1b_import_name']
        assert tbls2.ExampleTbl1.df.columns.tolist() == lst
        lst = ['date2_import_name', 'col_dummy', 'col_2a_import_name', 'col_2c_import_name']
        assert tbls2.ExampleTbl2.df.columns.tolist() == lst


    def test_files_fixture(self, files):
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
