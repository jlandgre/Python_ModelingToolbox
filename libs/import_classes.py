#Version 11/19/2024; customize 5/29/25
import importlib
"""
This module enables instancing customized project classes by a single-line 
call to instance_project_classes() from a driver script. It saves doing
this with multiple import + instantiation statements in the calling script.
The return from instance_xxx_classes() is a tuple of the instanced classes.

instance_xxx_classes() functions can be customized for each project and 
within-project use case as needed.
"""
def instance_model_classes(IsTest=False, IsParse=False, IsModel=False):
    """
    Instance customized [production-mode] classes for sales model Jupyter notebook
    JDL 11/20/24; customized 3/4/25
    """
    #Tuples of libs aka *.py filename, module/class name) 
    mods_cls_names = [('libs.projfiles', 'Files'), 
                      ('libs.projtables', 'ProjectTables'),
                      ('libs.sales_model', 'SalesModel')]
    
    #Create a dict of class objects (not yet instanced)
    class_objs = create_class_objs_dict(mods_cls_names)

    #Set a temp name for each class object and then use it to instance the class
    Files = class_objs['Files']
    files = Files(proj_abbrev='', subdir_home='most_recent', \
        IsTest=IsTest, subdir_tests='tests')

    #UseColInfo causes .__init__() to import col_info.xlsx making it available
    #to Table objects
    ProjectTables = class_objs['ProjectTables']
    tbls = ProjectTables(files, UseTblInfo=False, UseColInfo=True, IsPrint=False)

    #Custom sales model
    SalesModel = class_objs['SalesModel']
    model = SalesModel()

    return files, tbls, model

def instance_dboard_classes(IsTest=False):
    """
    Instance customized [production-mode] classes for dashboard plots
    JDL 3/20/25
    """
    #Tuples of libs aka *.py filename, module/class name) 
    mods_cls_names = [('libs.projfiles', 'Files'), 
                      ('libs.projtables', 'ProjectTables'),
                      ('libs.parse_fns', 'ParseImports'),
                      ('libs.dashboard', 'DashboardPlots')]
    
    #Create a dict of class objects (not yet instanced)
    class_objs = create_class_objs_dict(mods_cls_names)

    #Set a temp name for each class object and then use it to instance the class
    Files = class_objs['Files']
    files = Files(proj_abbrev='', subdir_home='most_recent', \
        IsTest=False, subdir_tests='')

    ProjectTables = class_objs['ProjectTables']
    tbls = ProjectTables(files, IsParse=True)

    ParseImports = class_objs['ParseImports']
    parse = ParseImports()

    DashboardPlots = class_objs['DashboardPlots']
    dshbrd = DashboardPlots()

    #Custom parsers for ads and sales data
    ParseImports = class_objs['ParseImports']
    parse = ParseImports()

    return files, tbls, parse, dshbrd, parse

def create_class_objs_dict(mods_cls_names):
    """
    Return a dict of class objects to instance for the project
    JDL 11/19/24; updated 5/29/25
    """
    # Iteratively import the specified classes (not yet instanced) as dict values
    class_objs = {}
    for mod_name, cls_name in mods_cls_names:
        
        # importlib imports specified module by name and sets module, equal to it
        module = importlib.import_module(mod_name)

        # getattr returns the specified class object from the module
        class_objs[cls_name] = getattr(module, cls_name)
    return class_objs