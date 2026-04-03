# Quota Table Setup And Dashboard Reporting

Internal Flask + SQLite app for market research operations. The app parses a two-sheet workbook format:

- `Question`
- `Data`

It preserves a 3-step workflow:

1. Upload and parse workbook
2. Configure quota layout
3. Display dashboard crosstab

## Workbook format

### Question sheet

- Header row: row 1
- Required columns:
  - `Name of items`
  - `Question type`
  - `Question(Matrix)`
  - `Question(Normal)`
- Numeric columns like `1`, `2`, `3` define answer code to answer label mappings

### Data sheet

- Variable headers: row 5
- Respondent data: row 8 onward
- Data row 5 variable codes link directly to `Question.Name of items`
- Coded answers are decoded through the Question sheet

## Key business rule

Respondents count into quota only when raw `Từ chối` is not normalized to `x`.

## Features

- Sheet detection for `Question` and `Data`
- Question dictionary parsing
- Respondent-level coded and decoded data
- Variable catalog with quota eligibility
- Step 2 horizontal and vertical selector tables
- Count / Percent / Count + Percent
- Percent mode:
  - Total %
  - Row %
  - Column %
- Step 3 business-friendly crosstab with totals
- Exports:
  - `quota_dashboard.xlsx`
  - `cleaned_data.xlsx`
  - `question_dictionary.xlsx`
  - `mapping_audit.xlsx`

## Run locally

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

### Optional AI setup for Step 2 suggestion

Edit local file:

- `C:\Users\ACER\Documents\New project\.env`

Example:

```env
OPENAI_API_KEY=your_new_key
OPENAI_MODEL=gpt-4o-mini
```

Open:

- [http://127.0.0.1:5000/upload](http://127.0.0.1:5000/upload)

## Tests

```powershell
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD='1'
python -m pytest
```

## Main routes

- `/upload` : Step 1 upload and parse summary
- `/configure-quota/<upload_id>` : Step 2 selector tables
- `/dashboard/<upload_id>` : Step 3 crosstab dashboard
- `/history` : upload history

## Export notes

The dashboard export follows the current displayed table and preserves:

- selected row variable
- selected column variable
- display mode
- percent mode
- accepted base size
- totals
