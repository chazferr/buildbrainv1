# BuildBrain Doc-to-Spreadsheet PoC

Extracts submission requirements and trades/scope from construction PDF drawing sets using Claude Vision API, then outputs a structured Excel workbook with full source traceability.

## Setup

```bash
# 1. Create a virtual environment (recommended)
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set your Anthropic API key
#    Option A: Environment variable
set ANTHROPIC_API_KEY=sk-ant-api03-your-key-here    # Windows cmd
#    export ANTHROPIC_API_KEY=sk-ant-api03-...       # bash/zsh

#    Option B: Create a .env file (copy from example)
copy .env.example .env
#    Then edit .env with your actual key
```

## Usage

### Web App (Recommended)

```bash
python app.py
```

Then open **http://localhost:5000** in your browser.

- Drag & drop one or more PDF drawing sets onto the page
- Click **Submit Bid**
- Watch real-time progress as each page is analyzed
- Download the resulting Excel workbook when done

### CLI

```bash
python extract_to_excel.py
```

#### Required PDFs (CLI only)

Place these PDFs on your Desktop (or set `BUILDBRAIN_PDF_DIR` env var):
- `Site Drawings.pdf`
- `Midstate House Drawings.pdf`

The script auto-detects the Desktop location on Windows.

## Output

Produces `buildbrain_output.xlsx` with three tabs:

| Tab | Description |
|-----|-------------|
| **Submission_Requirements** | All submission/permit/inspection requirements found |
| **Trades_and_Scope** | All trades implied by drawings, notes, and specs |
| **Source_Trace** | Every row traced back to PDF name + page number + evidence |

## Output Schema

### Submission_Requirements
| Column | Type | Description |
|--------|------|-------------|
| category | string | e.g. Permits, Submittals, Inspections |
| requirement | string | Description of the requirement |
| mandatory | boolean | True if clearly mandatory |
| source_pdf | string | Source PDF filename |
| source_page | int | 1-indexed page number |
| evidence | string | Short quote from the page |

### Trades_and_Scope
| Column | Type | Description |
|--------|------|-------------|
| csi_division | string | 2-digit CSI division or "NA" |
| trade | string | Trade name |
| scope_description | string | Brief scope description |
| source_pdf | string | Source PDF filename |
| source_page | int | 1-indexed page number |
| evidence | string | Short quote from the page |

### Source_Trace
| Column | Type | Description |
|--------|------|-------------|
| row_type | string | "requirement" or "trade" |
| category_or_trade | string | Category name or trade name |
| source_pdf | string | Source PDF filename |
| source_page | int | 1-indexed page number |
| evidence | string | Short quote from the page |

## Architecture

```
buildbrain_poc/
  ├── app.py                  # Flask web server
  ├── engine.py               # Shared extraction logic
  ├── extract_to_excel.py     # CLI version
  ├── templates/
  │   └── index.html          # Drag-and-drop web UI
  ├── uploads/                # Temp upload dir (auto-created)
  ├── .env                    # API key
  ├── requirements.txt        # Python dependencies
  └── README.md
```

## Cost Tracking

After execution, a summary shows:
- Total pages processed
- Total input/output tokens
- Estimated cost (input @ $18/1M tokens, output @ $85/1M tokens)
- Per-PDF cost breakdown

## Error Handling

- If a page fails JSON parsing, it retries once with a corrective prompt.
- If it fails again, the page is skipped and logged.
- Failed pages are listed in the summary.
