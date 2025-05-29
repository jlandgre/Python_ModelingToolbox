#Customized 4/3/25
#Master Version 4/18/23 - Updated for projtables.ProjectTables instance and its inputs
#J.D. Landgrebe/Data-Delve Engineer LLC
#Covered under MIT Open Source License (https://github.com/jlandgre/Python_Projfiles)

import inspect, os

class Files():
    """
    Class for keeping track of project files in standard project folder structure

    __init__() Arguments:
      proj_abbrev [String] project prefix string for folder names
      subdir_home [String] name of a Home subdirectory within
        proj/proj_case_studies folder
      IsTest [Boolean] Toggles between production and testing (IsTest=True).
      subdir_tests [String] allows files related to specific issues to be placed
        in subfolders within the Proj/proj_scripts/tests subfolder.

        For example, if Issues List issue 42 relates to solving a bug, the folder
        "tests/issue_042_2023_10_BugFix" subfolder would contain example files to
        recreate the issue along with text or Word doc documentation exlaining
        the cause of the bug and how it was fixed

    Naming and Case Conventions:
    * lowercase for project-specific attributes except if creates ambiguity;
      then use underscore to separate words
    * self.path_xxx is a complete directory path with os-specific separator as
      last character
    * self.pf_xxx is a complete directory path + filename
    * self.f_xxx is a filename with extension
    * self.path_subdir_xxx is a folder name or directory path suffix

    JDL Updated 4/12/23
    """
    def __init__(self, proj_abbrev='', subdir_home='', IsTest=False, subdir_tests=''):
        self.IsTest = IsTest #Boolean toggle for test versus production mode
        #self.subdir_tests = subdir_tests
        self.proj_abbrev = proj_abbrev

        #Initialize Class attributes
        self.lstpaths = [] #Internal list of paths
        self.path_root = '' #Project root directory
        self.path_libs = '' #project scripts subdirectory
        self.path_tests = '' #tests subfolder within proj_scripts
        self.path_data = '' #project data subdirectory
        self.path_case_studies = '' #case_studies subdirectory
        self.subdir_home = '' #optional "home" for current activity (e.g. case_studies subfolder)
        self.subdir_tests = subdir_tests #optional path to tests subfolder
        self.path_subdir_home = '' #optional path to home subfolder (within proj_case_studies)
        self.pathfile_error_codes = '' #path to ErrorCodes.xlsx

        #Optional subdirectory within tests folder - to contain issue-specific files
        if IsTest: self.subdir_tests = subdir_tests
          
        #Set generic and project-specific paths
        self.SetGenericProjectPaths()
        self.SetProjectSpecificPaths()

        #Set hard-coded lists of files to import for each raw data source
        if not IsTest: self.SetProjectSpecificFileLists()

    def SetGenericProjectPaths(self):
        """
        Set strings for project-specific files and paths
        Updated 4/18/23
        """
        #Instance Project Paths and set top-level folder names and paths
        iLevels = 3

        self.BuildLstPaths(iLevels)
        self.path_root = self.lstpaths[1]
        self.path_libs = self.lstpaths[0]
        self.path_tests = self.path_root + 'tests' + os.sep
        self.path_case_studies = self.path_root + 'case_studies' + os.sep

        #If a home subdirectory was specified, set path_home
        self.path_home = self.path_root + 'analysis' + os.sep
        if len(self.subdir_home) > 0:
          self.path_subdir_home = self.path_home + self.subdir_home + os.sep

        #If testing, reassign root and data directories to tests folder locations
        if self.IsTest:
          self.path_data = self.path_tests
          self.path_home = self.path_tests

          #reassign data path if tests data subdirectory specified
          if len(self.subdir_tests) > 0:
            self.path_data = self.path_tests + self.subdir_tests + os.sep
            self.path_home = self.path_data
            self.path_subdir_home = self.path_data

        # For security, store user-specific credentials/tokens outside of root folder
        self.pf_credentials = self.path_root + 'credentials.csv'

        # ColInfo
        self.pf_col_info = self.path_libs + 'col_info.xlsx'
        if self.IsTest: self.pf_col_info = self.path_data + 'col_info.xlsx'

        #Error codes file location
        self.pathfile_error_codes = self.path_data + 'ErrorCodes.xlsx'

    def SetProjectSpecificPaths(self):
        """
        Project specific directories and files
        JDL Modified 3/31/25
        """
        pass

    def SetProjectSpecificFileLists(self):
      """
      project-specific filenames
      JDL 3/20/25
      """
      pass

    def BuildLstPaths(self, iLevels):
        """
        Build list of nested directory paths based on location of projfiles.py
        Uses inspect.getframeinfo() method
        JDL Updated 1/16/23
        """
        # List paths to iLevels levels - starting with home/top, lst[0]; 4/8/22
        PF_thisfile = inspect.getframeinfo(inspect.currentframe()).filename
        path_thisfile = str(os.path.dirname(os.path.abspath(PF_thisfile))) + os.sep
        lstdirs = path_thisfile.split(os.sep)

        self.lstpaths = []
        for i in range(len(lstdirs)-1, len(lstdirs) - iLevels-1, -1):
            self.lstpaths.append(os.sep.join(lstdirs[0:i]) + os.sep)
    
    def PrintLocations(self):
      print('\n')
      print('files.path_root\n', self.path_root, '\n')
      print('files.path_data\n', self.path_data, '\n')
      print('files.path_case_studies\n', self.path_case_studies, '\n')
      print('files.path_home\n', self.path_home, '\n')
      print('files.path_libs\n', self.path_libs, '\n')
      if self.IsTest:
        print('files.path_tests\n', self.path_tests, '\n')

