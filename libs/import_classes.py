#Version 11/19/2024; customize 9/18/26
import importlib
"""
This module enables instancing customized project classes by a single-line 
call to instance_project_classes() from a driver script. It saves doing
this with multiple import + instantiation statements in the calling script.
The return from instance_xxx_classes() is a tuple of the instanced classes.

instance_xxx_classes() functions can be customized for each project and 
within-project use case as needed.
"""
def instance_classes(IsTest=False, subdir_tests=''):
    """
    Instance customized classes for sales model Jupyter notebook
    JDL 11/20/24; customized 3/4/25
    """
    #Tuples of libs aka *.py filename, module/class name) 
    mods_cls_names = [('libs.projfiles', 'Files'), 
                      ('libs.projtables', 'ProjectTables')]
    
    #Create a dict of class objects (not yet instanced)
    class_objs = create_class_objs_dict(mods_cls_names)

    # Instance files
    Files = class_objs['Files']
    files = Files(proj_abbrev='', IsTest=IsTest, subdir_tests=subdir_tests)

    # Instance tbls
    #UseColInfo causes .__init__() to import col_info.xlsx to make available to Table objs
    ProjectTables = class_objs['ProjectTables']
    tbls = ProjectTables(files, UseTblInfo=False, UseColInfo=True, IsPrint=False)

    return files, tbls

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