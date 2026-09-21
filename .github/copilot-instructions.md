# Python Modeling Toolbox - AI Coding Instructions
updated 9/21/26

## Project Architecture

A reusable toolbox of classes for consulting-style Python modeling projects, emphasizing
metadata-driven configuration over hard-coded logic (e.g. import/parse instructions,
column renaming, dtype-setting are all data-driven via dicts/DataFrames, not per-file code).

Core classes (see `libs/`, described in `readme.md`):
- **`Files`** (`projfiles.py`, instanced as `files`) — standard/project-specific directory
  paths and file names. Naming convention: `path_xxx` (dir + trailing `os.sep`), `pf_xxx`
  (full path + filename), `f_xxx` (filename only), `path_subdir_xxx` (folder name/suffix).
- **`ProjectTables`** (`projtables.py`, instanced as `tbls`) — collection of `Table` objects
  plus project-level table metadata.
- **`Table`** (`projtables.py`, custom instance names, e.g. `tbls.Survey`) — one table's
  data (`.df`), pre-parse raw data (`.df_raw`/`.lst_dfs`), and column metadata (`.dfColInfo`).
- **`ColumnInfo`** (`col_info.py`, instanced as `cinfo`) — metadata for each table's
  variables (renaming, subsetting, dtypes, units, descriptions), sourced from an Excel file.
- Parsing classes (`parsetables.py`) — one class per raw-data layout (see below), each
  instanced with a `Table` instance as the constructor argument and exposing
  `ParseDfRawProcedure()` to populate `parse.df`.
- `import_classes.py` — single-call instancing of toolbox + project classes for a clean
  notebook/driver interface (e.g. `files, tbls = instance_classes(IsTest=True)`).
- `pd_util.py` — small pandas utility functions (e.g. `dfExcelImport`, `custom_info`).

## Data Import/Parse Configuration

Table import (`dImportParams`) and raw-data parsing (`dParseParams`/`parsetables.py`
parser classes) are configured via dicts, not hard-coded per table. For the full pattern,
parameter keys, and the parser class reference (`RowMajorTbl`, `ParseColMajorTbl`,
`InterleavedColBlocksTbl`), see the `data-import-parse` skill.

## Function/Method Architecture and Docstrings

**Every method follows this pattern** — a short "Procedure" method calling single-action
methods in sequence, each independently testable:

```python
def ParseDfRawProcedure(self):
    """
    Procedure to parse blocks of columns in self.df_raw and set self.df
    JDL 6/6/25
    """
    self.FindDataBoundaries()
    self.SetDfCategories()
    self.TransferAllCols()

def FindDataBoundaries(self):
    """
    Set data boundary indices for parsing using parse params and flag columns.
    JDL 6/6/25
    """
    ...
```

**Docstring requirements:**
- Short description of what the method does — never repeats the method name.
- Author/date on its own line: `JDL M/D/YY` for original; append `; updated M/D/YY` or
  `; Modified M/D/YY` for later edits (both forms are used interchangeably in this repo).
- No hyphens-line banner within methods (unlike the VBA convention) — plain triple-quoted
  docstring immediately after `def`.
- Section banners between classes/major groups of methods use a full-width comment block:
  ```python
  """
  ================================================================================
  ClassName Class - one-line description
  JDL date
  ================================================================================
  """
  ```

**Key requirements:**
- Methods operate on `self.*` attributes set in `__init__` or by prior methods in the
  procedure sequence — avoid passing many positional args between steps.
- Favor small, single-purpose methods (e.g. `SetDfCategories`, `TransferAllCols`,
  `ReadWriteColData`) over one large method, so each can have a dedicated test.
- Iteration state (e.g. `self.idx_col_cur`, `self.sht`, `self.pf`) is stored as an instance
  attribute set by the loop, not passed as a parameter, so sub-methods can reference it.
- Guard/default patterns use small helpers like `SetParseParam`/`SetImportParam` to read a
  dict key with a fallback default, rather than repeating `dict.get(...)` inline.

## General Code Style Guidelines

- Imports at top of file; `sys.path` is amended before project-local imports when a file
  needs to resolve `libs/` (see the `path_libs` pattern in `parsetables.py`/`projtables.py`
  and `tests/test_parsetables.py`).
- Boolean flags/params are named `Is*` (e.g. `IsPrint`, `IsAddFilenameCol`, `IsTest`,
  `is_unstructured`) — capitalized `Is` prefix for class/instance-level flags, lowercase
  `is_` for dict keys/local parse params.
- Prefer `pd.concat([...], ignore_index=True)` over in-place row appends.
- Avoid pandas operations that trigger `FutureWarning`s around implicit dtype downcasting
  (e.g. avoid `.fillna()` when it would silently change dtype); build a plain Python list
  of converted values and assign the whole column at once instead of writing values back
  into an existing, differently-typed column cell-by-cell.
- Comment on *why*, not *what* — inline comments call out non-obvious parsing logic (e.g.
  "Stop if blank header cell") rather than restating the code.

## Testing Framework

Tests use **pytest**, organized by class-under-test with one `TestXxx` class per source
class/major method group, mirroring `libs/*.py` in `tests/test_*.py`.

**Fixtures build up the exact object under test, layered:**
```python
@pytest.fixture
def files():
    return Files('tbls', IsTest=True, subdir_tests=subdir_tests)

@pytest.fixture
def tbl1_survey(files, dParseParams_tbl1_survey):
    """
    Table object for survey data
    JDL 9/25/24; Modified 5/30/25
    """
    d = {'ftype': 'excel', 'import_path': files.path_data, 'sht': 'raw_table'}
    tbl = Table('tbl1_survey', dImportParams=d, dParseParams=dParseParams_tbl1_survey)
    tbl.ImportToTblDf(lst_files='tbl1_survey.xlsx')
    return tbl

@pytest.fixture
def row_maj_tbl1_survey(tbl1_survey):
    """
    Instance RowMajorTbl parsing class for one raw df in survey data
    JDL 9/25/24; Modified 5/30/25
    """
    # Simulate iteration df from .lst_dfs
    tbl1_survey.df_raw = tbl1_survey.lst_dfs[0]
    return parsetables.RowMajorTbl(tbl1_survey)
```
- `files` always uses `IsTest=True` with a `subdir_tests` pointing at a `tests/test_data*`
  folder containing the fixture Excel/CSV files.
- Since parsing methods normally run inside a `for self.df_raw in self.lst_dfs:` loop,
  fixtures simulate one iteration by manually setting `tbl.df_raw = tbl.lst_dfs[0]` before
  instancing the parser class.
- Test-only param dicts (e.g. `dParseParams_tbl1_survey`) are their own fixture when reused
  across multiple table fixtures.

**Test methods:**
```python
class TestParseColMajorTbl:
    def test_FindDataBoundaries(self, parse_cm):
        """
        Set data boundary indices for parsing using parse params and flag columns.
        JDL 6/6/25
        """
        parse_cm.FindDataBoundaries()
        assert parse_cm.idx_header_row == 4
        assert parse_cm.idx_data_start == 5
        assert parse_cm.idx_data_end == 6
```
- Test docstring is a verbatim (or near-verbatim) copy of the method-under-test's docstring
  description line, plus a `JDL M/D/YY` date line.
- One test method per behavior/method-under-test; assert concrete expected values computed
  by hand from the fixture's raw test data file, not just types/shape.
- A module-level `IsPrint` flag (default `False`) gates optional `print()` debug output left
  in tests for future troubleshooting:
  ```python
  if IsPrint: print('\n', parse_cm.df, '\n')
  ```
- Module-level helper functions (not fixtures) handle repeated test-only logic, each with
  its own docstring, e.g.:
  ```python
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
  ```
- Run tests from the `tests/` folder (a comment at the top of each test file documents the
  exact invocation), e.g.:
  ```python
  #python -m pytest test_parsetables.py -v -s
  ```

## File Organization
- **`libs/`** — toolbox and project classes (`projfiles.py`, `projtables.py`,
  `parsetables.py`, `col_info.py`, `pd_util.py`, `import_classes.py`).
- **`tests/`** — one `test_*.py` per corresponding `libs/*.py` file; `tests/test_data*/`
  subfolders hold fixture Excel/CSV files referenced by `files.path_data`.
- **`readme.md`** — architecture overview and detailed parameter reference for
  `dImportParams`/`dParseParams` keys; update this alongside any new/changed parser or
  parameter.
- **`demo.ipynb`** — example notebook usage of the toolbox via `import_classes.py`.

## Naming Conventions
- **Classes**: PascalCase (e.g. `ProjectTables`, `ColumnInfo`, `RowMajorTbl`).
- **Methods**: PascalCase, verb-first, `*Procedure` suffix for top-level orchestration
  methods (e.g. `ParseDfRawProcedure`, `ImportToTblDf`).
- **Instance variables holding class instances**: lowercase, short (`tbls`, `files`,
  `cinfo`, `parse`).
- **Dicts of parameters**: `dImportParams`, `dParseParams`, `dParseParams_<table>` (tests).
- **DataFrames**: `df` (parsed/final), `df_raw` (pre-parse), `df_temp`/`df_block`
  (in-progress within a loop), `lst_dfs` (list of raw dfs pending parsing/concatenation).
- **Column-index/row-index parse params**: `icol_*` (column index), `idx_*` (row index
  computed at runtime), `n_*`/`nrows_*` (counts/offsets), `flag_*` (literal match values).
