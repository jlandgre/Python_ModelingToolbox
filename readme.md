# Python Modeling Toolbox

This describes a standard modeling toolbox useful for consulting work in Python. The toolbox consists of classes that manage tabular data, metadata about the tables and about individual variables, and error handling/user messaging for a model or other application. For consulting projects based in Python, Microsoft Excel usually plays a helpful role either in input files from test labs or other sources or as a way to display formatted output to non-coding users. Our toolbox includes a library of functions that streamline use of OpenPyXL.

The toolbbox's emphasis on systematic management of metadata allows common tasks to be configuration rather than hard-coded. Examples are configurations read from input files with instructions for data import, parsing input files into rows/columns DataFrames, column renaming and subsetting post-import, setting data types for variables and setting DataFrame indices.

### Overview of Classes and Utility Libraries

| Class Name     | File Name          | Instanced As | Description |
|----------------|--------------------|--------------|-------------|
| `ProjectFiles` | `projfiles.py`     | `files`      | Stores standard and project-specific directory paths and file names. |
| `ProjectTables`| `projtables.py`    | `tbls`       | Stores collection of `Table` objects and a DataFrame of table metadata, such as import and parsing instructions. |
| `Table`        | `projtables.py`    | Custom names | Stores data and metadata about an individual table including its `df`, `dfRaw` (freshly imported/pre-parsing), and `dfColinfo` with metadata about individual variables. |
| `ColumnInfo`   | `col_info.py`           | `col_info`   | DataFrame and methods for metadata about each table's variables, including info for renaming and subsetting imported columns, and for setting data types post-import. Also contains documentation about variables such as units and description. |
| `ErrorHandling`| `error_handling.py`| `errs`       | Metadata and methods related to detecting and reporting errors and providing warnings and guidance to model's users|
| *N/A*          | `util_openpyxl.py` | *N/A*        | Utility functions for writing and formatting data in Microsoft Excel. |
| *N/A*          | `import_classes.py` | *N/A*        | Used for efficient instancing of toolbox and project-specific classes |

### Project-Specific Internal Architecture

A best practice is for project-specific class(es) to be architected to divide code into "procedures," which are class methods containing a step-by-step, linear recipe of calls to individual methods to complete a larger task.

Individual methods called by procedures are single-action and are each validated by at least one Pytest test function in a separate file. Usually, project class methods have `tbls` and `errs` as arguments in addition to `self`.

<p align="center">
Example Model Architecture (With File and Method Names)</br></br>
<img src="images/architecture_diagram.png" alt="Best practice architecture" width="600"/>
</p>

### Presenting A Clean User Interface for Models

As a convenience and to present a clean user interface for running a model, `import_classes.py` can be customized to instance toolbox and project classes with a single statement and with customizable, hard-coded parameters out of view of the model's user. For example, it handles the detail that `files` can be instanced with IsTest=True in production mode or False to point to sandbox test data. The code snippet shows a Jupyter notebook cell from a project that instances toolbox classes and a project-specific `model` instance. It does this with minimal lines to avoid distracting the user from the business of running the model.

```python
# Import libraries/instance helper classes. This instances files, tbls and model.
import pandas as pd, numpy as np, datetime as dt
from libs.import_classes import instance_project_classes
files, tbls, model = instance_project_classes(IsParse=True)
```

J.D. Landgrebe,
Data Delve LLC, May 2025

---

## User Guide for `Table.ImportToTblDf` Method
The `Table.ImportToTblDf` method is used to import data from various file types (e.g., Excel, CSV, Feather) into a `Table` instance. It supports structured and unstructured data imports, with options to customize the behavior using parameters in `dImportParams` and `dParseParams`.

The code is within projtables.py that contains two foundational classes that are part of a modeling toolbox:
* `ProjectTables` (typically instanced as `tbls`) has attributes of all data tables and their metadata associated with a project. `Table` objects can either be individually instanced or automatically instanced by project-specific code in `ProjectTables.InstanceTblObjs`. The example below is for a hypothetical table to be imported from the sheet, `data` in `Example1.xlsx`.  The `lst_files` dict item can be an individual filename or a list of files, and it can be set dynamically as needed such as the situation where the files to be imported are in a sweep folder.
```python
  dImportParams = {'import_path':files.path_data, 'ftype':'excel', 'sht':'data'}
  dImportParams['lst_files'] = 'Example1.xlsx'
  self.ExampleTbl1 = Table('ExampleTbl1', dImportParams=dImportParams,
                                          dParseParams=None)
```
That code specifies how ExampleTbl1 data will be imported to its .df attribute. This method call performs the import for this situation where the Excel sheet is already rows/columns format requiring no separate parsing to rearrange the imported data. 
```python
  tbls.ExampleTbl1.ImportToTblDf()
```
* The `Table` class objects that are `tbls` attributes contain all metadata for a project table including its `.df` data and its `.name`. The latter is input as an argument in the example above, and a best practice is to name the table the same as its programmatic instance. Other attributes are `.dImportParams` and `.dParseParams` that describe how to import and parse data into `.df` for use in modeling and analysis. Data ingestion directly imports to `.df` for "structured" rows/columns raw data. If the data are unstructured but in a repeatable format, `ImportToTblDf` populates `Table.lst_dfs` with individual raw (unparsed) imported df's --enabling subesquent parsing and concatenation into `Table.df`.

---
The following sections describe Table.ImportToTblDf() inputs for ingesting and parsing data using .dImportParams and .dParseParams dictionaries.

### 1. `dImportParams`
This dictionary specifies import-related parameters.

| **Key**          | **Description**                                                                 | **Required/Optional** | **Default Value** |
|-------------------|---------------------------------------------------------------------------------|------------------------|-------------------|
| `ftype`           | File type to import. Supported values: `'excel'`, `'csv'`, `'feather'`.         | Required               | None              |
| `lst_files`       | List of file paths or a single file path to import.                             | Required               | None              |
| `import_path`     | Path to prepend to file names in `lst_files`.                                   | Optional               | None              |
| `sht`             | Sheet name or index for Excel files.                                           | Optional               | `0` (first sheet) |
| `sht_type`        | Specifies how to handle sheets in Excel files. Supported values: `'single'`, `'all'`, `'list'`, `'regex'`, `'startswith'`, `'endswith'`, `'contains'` (only `single` and `all` enabled as of 4/14/25). | Optional | `'single'`  |

---

### 2. `dParseParams`
This dictionary specifies parsing-related parameters.

| **Key**           | **Description**                                                                 | **Required/Optional** | **Default Value** |
|-------------------|---------------------------------------------------------------------------------|------------------------|-------------------|
| `is_unstructured` | Indicates whether the data is unstructured. If so, Table.lst_dfs is output; otherwise Table.df                                     | Optional               | `False`           |
| `n_skip_rows`     | Number of rows to skip at the top of the file (for structured data only).             | Optional               | `0`               |
| `parse_type`      | Name of parse class (in parsetables.py) for converting data in an unstructured but repeatable format. Currently supported values: `'none'`, `'RowMajorTbl'`, `'InterleavedColBlocksTbl'`. The parse Class must contain a `ParseDfRawProcedure()` method as described below in `ParseRawData` documenation| Optional | `'none'` |
| parser-specific params      | Varies by `parse_type`| Required | NA |
---

### 3. Behavior Based on `ftype`

#### 3.1 `ftype = 'excel'` Imports data from Excel files.
- **Parameters**:
  - `is_unstructured`: If `True`, the first row is not treated as headers (`header=None`), and imported (non-parsed) .df's are output in Table.lst_dfs for subsequent parsing.
  - `n_skip_rows`: Number of rows to skip at the top of the file (is_unstructured=True only).
  - `sht`: Specifies the sheet to import. Can be a name or index.
  - `sht_type`: Determines how sheets are handled:
    - `'single'`: Imports a single sheet specified by `sht`.
    - `'all'`: Imports all sheets in the workbook.
    - `'list'`: Imports sheets specified in a list.
    - `'regex'`: Imports sheets matching a regular expression.
    - `'startswith'`: Imports sheets whose names start with a specific string.
    - `'endswith'`: Imports sheets whose names end with a specific string.
    - `'contains'`: Imports sheets whose names contain a specific substring.

#### 3.2 `ftype = 'csv'` Imports data from CSV files.
- **Parameters**:
  - `is_unstructured`: If `True`, the first row is not treated as headers (`header=None`), and imported (non-parsed) .df's are output in Table.lst_dfs for subsequent parsing.
  - `n_skip_rows`: Number of rows to skip at the top of the file (is_unstructured=True only).

#### 3.3 `ftype = 'feather'` (not implemented as of 4/13/25)
- **Description**: Imports data from Feather files.
- **Additional Parameters**:
  - None

---

### 4. ImportToTblDf Examples

#### 4.1 Importing a Single Excel Sheet
```python
dImportParams = {'ftype':'excel', 'sht':'data', 'sht_type':'single'}
dImportParams['lst_files'] = 'Example2.xlsx'
tbl = Table('ExampleTable', dImportParams)
tbl.ImportToTblDf()
```

#### 4.2 Importing All Sheets from an Excel File
```python
dImportParams = {'ftype': 'excel', 'sht_type': 'all'}
dImportParams['lst_files'] = 'Example2.xlsx'
tbl = Table('ExampleTable', dImportParams)
tbl.ImportToTblDf()
```

#### 4.3 Importing a CSV File with Skipped Rows
```python
dImportParams = {'ftype':'csv'}
dImportParams['lst_files'] = 'Example3.csv'
dParseParams = {'n_skip_rows': 2}
tbl = Table('ExampleTable', dImportParams, dParseParams)
tbl.ImportToTblDf()
```

#### 4.4 Importing Unstructured CSV Data
```python
dImportParams = {'ftype': 'csv'}
dImportParams['lst_files'] = 'Example4.csv'
dParseParams = {'is_unstructured': True}
tbl = Table('ExampleTable', dImportParams, dParseParams)
tbl.ImportToTblDf()
```
---

#### 5. Default Behavior
- If `lst_files` is not specified as override argument, the method uses `dImportParams['lst_files']`.
- If `sht` is not specified for Excel files, the first sheet (`0`) is used.
- If `is_unstructured` is not specified, the data is treated as structured.

---

#### 6. Error Handling
To be implemented as of 4/13/25
- **Missing `ftype`**: Raises an error if `ftype` is not provided in `dImportParams`.
- **Invalid `ftype`**: Raises an error if `ftype` is not one of the supported values (`'excel'`, `'csv'`, `'feather'`).
- **File Not Found**: Raises an error if a file in `lst_files` does not exist.
- **Invalid Sheet Name**: Raises an error if the specified sheet does not exist in the Excel file.

## User Guide for `Table.ParseRawData` Method
The method shown below converts a list of df's containing unstructured data to a concatenated rows/columns Dataframe. It takes the Table instance (aka `self` in the code) as its sole argument. ParseRawData instances the parse class specified by Table.dParseParams['parse_type'] whose string value should be the name of a class in parsetables.py. The parse class must contain a ParseDfRawProcedure method that uses Table.df_raw as input to populate parse.df. This architecture and its use of `getattr` means that new parse classes can be added to parsetables.py as needed without modification to .ParseRawData().

```python
    import parsetables

    def ParseRawData(self):
        """
        Procedure to parse raw data for a given Table instance
        Updated 5/30/25
        """
        for self.df_raw in self.lst_dfs:

            # instance parse class with tbl (aka self) as argument and parse
            parse = getattr(parsetables, self.dParseParams['parse_type'])(self)
            parse.ParseDfRawProcedure()
                
            # Concatenate parsed data to tbl.df
            self.df = pd.concat([self.df, parse.df], ignore_index=True)
```

Example .ParseDfRawProcedure() for 
J.D. Landgrebe, Data Delve LLC
April 12, 2025; Updated 5/29/25