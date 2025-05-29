#Version 4/3/25
import pandas as pd
import numpy as np
# 4/3/25 commented out references to util and pd_util libraries

class ColInfo():
    """
    ColInfo Class for handling project variables' metadata
    Instancing populates four attributes:
    * dftable - DataFrame ColInfo row subset for specified sColSelectFlags
    * lst_KeepRaw - list of raw data variables to keep on import
    * lst_KeepFinal - list of final variables after de-nesting and calc variables
    * dict_Nested - dict of dictionaries; keep vars for subnested JSON
    JDL 9/10/21 Modified 1/11/23
    """
    def __init__(self, files, tbls):
        #self.sPathColInfo = sPathColInfo
        #self.sColSelectFlags = sColSelectFlags

        #Column names in colinfo.xlsx
        self.col_name = 'name'
        self.col_type = 'type'
        self.col_nested = 'nested_parent'
        self.col_importname = 'name_import'
        self.col_calc = 'IsCalculated'
        self.col_IsIndex = 'IsIndex'
        self.col_defaultval = 'val_default'

        self.dftable = self.CreateDFTable()

        #Read columns for either single or multi-index
        self.sColIndex = self.Set_sColIndex()
        self.lstMultiindex = None
        self.IsMultiIndex = False
        if self.sColIndex is None: 
            self.lstMultiindex = self.Set_lstMultiIndex()
            self.IsMultiIndex = True

        #Set lists of keep columns and dict_Nested for JSON variables
        self.lst_KeepRaw = self.BuildVarList(True, True, False, True, False, True)
        self.lst_KeepImported = self.BuildVarList(True, False, False, True, False, False)
        self.lst_KeepFinal = self.BuildVarList(True, False, True, False, True, False)
        self.dict_Nested = self.Set_dict_Nested()

        #Set lists of flag [Boolean] and numeric, non-calculated columns (6/21/22)
        self.lst_FlagCols, self.lst_NumericCols = [], []
        for col in self.lst_KeepImported:
            sType = (self.dftable['type'].loc[col]).lower()
            IsBool = (sType[0:7] == 'np.bool') | (sType == 'bool')
            if IsBool: self.lst_FlagCols.append(col)

            IsNum = (sType[0:6] == 'np.int') | (sType == 'int') | (sType == 'float')
            IsNum = IsNum | (sType[0:6] == 'pd.int')
            if IsNum: self.lst_NumericCols.append(col)

        #set list of nested sub-variables (5/6/22)
        self.lst_Nested = []
        for key in self.dict_Nested:
            subdict = self.dict_Nested[key]
            for subkey in subdict:
                self.lst_Nested.append(subdict[subkey])

    def RecodeFlagColsToBool(self, df):
        """
        Recode all flag (boolean) columns from 1/blank to Boolean coding
        JDL 5/22/22 - not validated xxx
        """
        fil = self.dftable[self.col_type] == 'bool'
        lst_flags = list(self.dftable[fil].index)

        for sCol in lst_flags:
            #df = pd_util.RecodeFlagColToBoolean(df, sCol)
            pass

    def Set_sColIndex(self):
        """
        Get index column name --if single selection in ColInfo
        JDL 10/6/21
        """
        fil = ~self.dftable[self.col_IsIndex].isnull()

        #Return if single index
        if self.dftable[fil].index.size == 1:
            return list(self.dftable[fil].index)[0]
        else:
            return None

    def Set_lstMultiIndex(self):
        """
        Get list of multiindex column names --if multiple ColInfo selections
        JDL 10/25/21
        """
        fil = ~self.dftable[self.col_IsIndex].isnull()

        #If multiple ColInfo rows specified as indices, return sorted list
        if self.dftable[fil].index.size > 1:
            df_idx = self.dftable[fil].sort_values(self.col_IsIndex)
            return list(df_idx.index)

    def Set_dict_Nested(self):
        """
        Build ColInfo dictionary of dictionaries for nested variables : 
              keys: unique import names from col_nested
              values: sub-dicts mapping sub-variable import name (key) to 
                      ColInfo variable name (values)
        """
        df = self.CISubsetTableNestedVars()
        dictTemp = {}

        #Build a dictionary for each nested variable
        for varNested in df[self.col_nested].unique():

            #Subset DF to current nested variable
            dfVar = df[df[self.col_nested]==varNested]

            #Create dict to lookup var name from its import (nested) name
            lstVars = dfVar.index.to_list()
            lstNested = dfVar[self.col_importname].to_list()

            #Add variable's dictionary to master dictionary for the table
            dictTemp[varNested] = dict(zip(lstNested, lstVars))
        return dictTemp

    def CreateVarFilters(self):
        """"
        Create ColInfo filters for various types of variables
        JDL 10/1/21
        """
        dfCI = self.dftable.copy()
        fil_temp = dfCI[self.col_calc] == 'temp'
        fil_calc = dfCI[self.col_calc] == 'calc'

        lst_nested = dfCI[self.col_nested].dropna().unique()
        fil_nested = dfCI.index.isin(lst_nested)

        fil_subnested = ~dfCI[self.col_nested].isnull()

        #base = none of the above
        fil_base = ~fil_temp & ~fil_calc & ~fil_nested & ~fil_subnested

        #return as tuple of boolean series
        return fil_base, fil_temp, fil_calc, fil_nested, fil_subnested

    def BuildVarList(self, IsBase, IsTemp, IsCalc, IsNested, IsSubNested, 
                IsImportNames):
        """
        Build var list excluding specified types
        JDL 10/1/21
        """
        lst_bools = [IsBase, IsTemp, IsCalc, IsNested, IsSubNested]
        #Create filters for different types of variables
        lst_filters = list(self.CreateVarFilters())
        #fil_base, fil_temp, fil_calc, fil_nested, fil_subnested = tup

        #Filter out rows excluded by Boolean args
        fil = pd.Series(index=self.dftable.index, data=True)
        for IsInclude, subfilter in zip(lst_bools,lst_filters):
            if not IsInclude: fil = fil & (~subfilter)

        #Return either variable name or its import name
        if IsImportNames:
            return list(self.dftable[fil]['name_import'])
        else:
            return list(self.dftable[fil].index)

    def BuildLstCIVarsTypes(self):
        """
        Build lists of variable names and data types for specified table
        JDL 9/10/11
        """

        #Filter out ColInfo rows for drop variables: either nested JSON 
        # or temporary variables that end up getting anonymized or 
        # #converted to calcs
        fdum1, fil_temp, fdum2, fil_nested, fdum3 = self.CreateVarFilters()
        df = self.dftable[~fil_nested & ~fil_temp]

        #Create lists of variables and data types
        self._lst_TblVars = list(df.index)
        self._lst_TblTypes = [eval(x) for x in df[self.col_type]]

    def CISubsetTableNestedVars(self):
        """
        Subset colinfo DF to nested sub-var rows
        """
        df = self.dftable.copy()
        df = df[~df[self.col_nested].isnull()]
        return df
    
    def BuildNestedVarDict(self):
        """
        Build ColInfo dict of dicts for nested variables for specified table: 
              keys: unique import names from col_nested
              values: sub-dicts mapping sub-variable import name (key) to 
                      ColInfo variable name (values)
        """
        df = self.CISubsetTableNestedVars()

        #Build a dictionary for each nested variable
        for varNested in df[self.col_nested].unique():

            #Subset DF to current nested variable
            dfVar = df[df[self.col_nested]==varNested]

            #Create dict to lookup var name from its import (nested) name
            lstVars = dfVar.index.to_list()
            lstNestedNames = dfVar[self.col_importname].to_list()

            #Add variable's dictionary to master dictionary for the table
            self.dict_Nested[varNested] = dict(zip(lstNestedNames, lstVars))

    def RenameDFColsFromImport(self, df):

        #Columns
        for col in df:
            if col in self.dftable['name_import'].values:
                sName = self.dftable[self.dftable['name_import'] == col].index[0]
                df = df.rename(columns={col:sName})
        
        #Index/Multiindex Name(s) - build list of either current or renamed index names
        if df.index.names[0] is None: return df
        lst_new = []
        for idxname in df.index.names:
            if idxname in self.dftable['name_import'].values:
                sName = self.dftable[self.dftable['name_import'] == idxname].index[0]
                lst_new.append(sName)
            else:
                lst_new.append(idxname)
        df.index.names = lst_new
        return df

    def CreateDFTable(self):
        """
        Create sorted ColInfo subset for specified table
        JDL 10/18/21 (converted from property)
        """
        df = pd.read_excel(self.sPathColInfo, sheet_name='colinfo', 
            index_col='name')

        df = df[~df[self.sColSelectFlags].isnull()]
        df = df.sort_values(self.sColSelectFlags, axis=0)
        return df

    def SetDefaultVals(self, df, IsImportNames, lstCols=[]):
        """
        Set DF col default values based on ColInfo
        JDL 11/29/21
        """
        if not 'val_default' in self.dftable.columns: return df
        if len(lstCols) == 0: lstCols = list(df.columns)
        for col in lstCols:

            #Get the CI index (variable name) of the variable's row in CI
            if IsImportNames:
                fil = self.dftable['name_import'] == col
            else:
                fil = self.dftable.index == col
            if self.dftable[fil].index.size != 1: continue
            idx = self.dftable[fil].index[0]

            #Skip if column is not in ColInfo or if no default value
            val_default = self.dftable.loc[idx, 'val_default']
            #if util.IsNullVal(val_default): continue

            #If value is NaN or 'nan', replace with default value
            fil = (df[col].isnull()) | (df[col]=='nan')
            df.loc[fil, col] = val_default
        return df

    def SetTypes(self, df, IsImportNames, lstCols=[]):
        """
        Set DF col variable types based on ColInfo
        JDL 11/29/21
        """
        if not 'type' in self.dftable.columns: return df
        if len(lstCols) == 0: lstCols = list(df.columns)

        for col in lstCols:
            
            #Get the CI index (variable name) of the variable's row in CI
            if IsImportNames:
                fil = self.dftable['name_import'] == col
            else:
                fil = self.dftable.index == col
                
            #Skip if column is not in ColInfo or if no default value
            if self.dftable[fil].index.size != 1: continue
            idx = self.dftable[fil].index[0]

            dtype = self.dftable.loc[idx, 'type']
            #if util.IsNullVal(dtype): continue
            
            df[col] = df[col].astype(eval(dtype))
        return df

    def RemoveIndexColsFromLst(self, lst):
        """
        Remove index column names from a list (to process just df.columns etc.)
        JDL 1/11/23
        """
        lst_new = lst.copy()
        if not self.sColIndex is None:
            lst_remove = self.sColIndex
        elif not self.lstMultiindex is None:
            lst_remove = self.lstMultiindex
        else:
            return

        for col in lst_remove:
            if col in lst: lst_new.remove(col)
        return lst_new