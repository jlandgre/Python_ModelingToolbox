---
name: data-import-parse
description: Configure Table import (dImportParams) and raw-data parsing (dParseParams) in projtables.py/parsetables.py, or choose/extend a parser class in parsetables.py. Use when creating or editing a Table's import/parse configuration, adding a new raw-data layout, or asking how ImportToTblDf/ParseRawData work.
---

# Data Import/Parse

Tables are configured, not coded: import and parsing behavior is driven entirely by two
dicts passed to `Table.__init__`, so a new raw-data source is usually a config change, not
new logic.

## Quick start

```python
d = {'ftype': 'excel', 'sht': 'raw_table', 'import_path': files.path_data}
d2 = {'is_unstructured': True,
      'parse_type': 'RowMajorTbl',
      'flag_start_bound': 'Answer Choices',
      'flag_end_bound': '<blank>',
      'icol_start_bound': 0,
      'icol_end_bound': 0,
      'iheader_rowoffset_from_flag': 0,
      'idata_rowoffset_from_flag': 1,
      'block_id_vars': ('question_text', -2, 0)}
tbls.Survey = Table('tbl1_survey', dImportParams=d, dParseParams=d2)
tbls.Survey.ImportToTblDf(lst_files='tbl1_survey.xlsx')
tbls.Survey.ParseRawData()
```

## How it works

- `dImportParams` controls file ingestion: `ftype` (`'excel'`/`'csv'`/`'feather'`),
  `lst_files`, `import_path`, `sht`, `sht_type`.
- `dParseParams` controls parsing raw/unstructured data into rows/cols: `is_unstructured`,
  `parse_type`, plus parser-specific keys (see below).
- `ImportToTblDf`: if `is_unstructured` is falsy, populates `Table.df` directly (no parsing
  needed). If `is_unstructured=True`, populates `Table.lst_dfs` (one raw df per
  file/sheet) instead, for `ParseRawData` to parse and concatenate into `Table.df`.
- `Table.ParseRawData` dynamically instances the parser class named by
  `dParseParams['parse_type']` via `getattr(parsetables, ...)(self)` — new parser classes
  in `parsetables.py` are automatically available with no dispatch code to update.
- Full parameter reference (all `dImportParams`/`dParseParams` keys, defaults,
  required/optional): see `readme.md` in the project root.

## Choosing/extending a parser class (`parsetables.py`)

Each raw-data layout has its own class. All follow the same shape: `__init__(self, tbl)`
reads `tbl.df_raw` and `tbl.dParseParams`; `ParseDfRawProcedure()` runs a linear sequence of
single-action methods that populate `self.df`.

| Class | Layout | Key params |
|---|---|---|
| `RowMajorTbl` | Repeating row-major blocks stacked vertically (e.g. SurveyMonkey export) | `flag_start_bound`/`flag_end_bound`, `icol_start_bound`/`icol_end_bound`; optional `block_id_vars` (list of `(name, row_offset, col_index)` tuples, extracted per block via `RowMajorBlockID`) |
| `ParseColMajorTbl` | Single data block, categories in column 0, data spread across columns | `flag_start_bound`/`flag_end_bound`, `icol_start_flag`/`icol_end_flag` (single start/end pair only) |
| `InterleavedColBlocksTbl` | Repeating column blocks (metadata columns + fixed-width data block, repeated) | `idx_start`, `n_cols_metadata`, `n_cols_block` |

`RowMajorTbl` supports a `'<blank>'` sentinel for `flag_end_bound`, matched via `.isnull()`
instead of literal equality — use this when a block ends at the first blank row rather than
a literal flag value.

**Prefer reusing/extending an existing parser class** for a new but similar raw layout
rather than writing ad hoc parsing code in `Table` or project-specific files. Only add a new
parser class in `parsetables.py` when the layout is genuinely different from the three above.

## Testing a new/changed parser

Follow the layered-fixture pattern already used for the existing parsers in
`tests/test_parsetables.py` (`files` → `Table` → parser instance, simulating one
`.lst_dfs` iteration by setting `tbl.df_raw = tbl.lst_dfs[0]` before instancing the parser).
See the `## Testing Framework` section of `copilot-instructions.md` for the general test
conventions (docstrings, `IsPrint`, helper functions) that apply here too.
