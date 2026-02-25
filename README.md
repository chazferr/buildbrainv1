# BuildBrain v1

Construction document extraction and pricing engine. Upload bid documents (PDFs, drawings, specs, addenda) and get a structured Excel workbook with trade-level pricing, project scalars, and full source traceability.

## Features

- **Multi-format ingestion** — PDF, DOCX, XLSX, CSV, EML, JPG, PNG, TXT, and more
- **3-tier PDF extraction** — pages classified as text/mixed/image; text-heavy pages skip Vision API to reduce cost
- **Pre-check flow** — quick scan of cover sheets to detect project type, unit count, SF, and construction type before full extraction
- **Scalar gate** — after extraction, review and confirm project dimensions (with confidence scoring) before Excel is generated
- **Commercial/multifamily routing** — system-type detection for HVAC, roofing, plumbing, and framing based on project type
- **Complexity multipliers** — renovation (1.35x), gut rehab (1.55x), addition (1.20x) applied automatically
- **Buyout/SOV mapping** — optional reference buyout spreadsheet for automatic trade SOV matching
- **Real-time progress** — SSE streaming shows extraction status page-by-page
- **Cost tracking** — per-file and total API cost breakdown (input/output tokens)

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

1. Drag & drop construction documents onto the page
2. Click **Submit Bid** — a pre-check scan detects project info from cover sheets
3. Review and adjust detected project type, unit count, SF, and construction type
4. Click **Confirm & Run Full Analysis** — pages are extracted with the 3-tier system
5. **Scalar gate** — review extracted project dimensions with confidence indicators before pricing
6. Click **Confirm & Generate Excel** — the pricing workbook is built
7. Download the resulting Excel workbook

### CLI

```bash
python extract_to_excel.py
```

Place PDFs on your Desktop (or set `BUILDBRAIN_PDF_DIR` env var).

## Output

Produces `buildbrain_output.xlsx` with multiple tabs:

| Tab | Description |
|-----|-------------|
| **Project Scalars** | Extracted project dimensions with confidence scores and override columns |
| **Buyout Summary** | Trade-level pricing with material, labor, and equipment breakdowns |
| **Submission_Requirements** | All submission/permit/inspection requirements found |
| **Trades_and_Scope** | All trades with CSI divisions, scope descriptions, and source tracing |
| **Source_Trace** | Every row traced back to PDF name + page number + evidence |
| **Addenda_Log** | Addendum findings and change tracking |
| **Failed_Pages** | Pages that could not be parsed, with error details |

### Project Scalars

Key dimensions extracted and scored:

| Scalar | Example | Detection Method |
|--------|---------|-----------------|
| project_type | multi_family | Cover sheet / scope analysis |
| construction_type | new_construction | Keywords in scope text |
| unit_count | 12 | Cover sheet / dwelling unit references |
| floor_count | 3 | Section drawings / notes |
| total_building_sf | 8,400 | Area calculations |
| footprint_sf | 2,800 | Slab/concrete scope or SF / floors |
| perimeter_lf | 212 | Explicit or estimated from footprint |
| roof_type | asphalt_shingle | Brand names (CertainTeed) and keywords |
| hvac_system_type | residential_minisplit | Brand names (Mitsubishi) and keywords |
| foundation_type | slab_on_grade | Scope descriptions |

## Architecture

```
buildbrain_poc/
  ├── app.py                  # Flask web server + scalar gate API
  ├── engine.py               # Extraction engine, pricing, Excel generation
  ├── extract_to_excel.py     # CLI version
  ├── wage_rates.py           # CT prevailing wage rate tables
  ├── material_db.py          # Material unit price database
  ├── productivity_rates.py   # Labor productivity rates by trade
  ├── templates/
  │   └── index.html          # Web UI (upload, pre-check, scalar gate, results)
  ├── uploads/                # Temp upload dir (auto-created, auto-cleaned)
  ├── .env                    # API key (not committed)
  ├── requirements.txt        # Python dependencies
  └── README.md
```

## Supported File Types

| Extension | Processing Method |
|-----------|-------------------|
| `.pdf` | 3-tier: text pages via text API, mixed/image pages via Vision API |
| `.jpg`, `.jpeg`, `.png`, `.tiff`, `.bmp`, `.webp` | Direct Vision API |
| `.docx`, `.doc` | Text extraction via python-docx |
| `.xlsx`, `.xls`, `.csv` | Text extraction via pandas/openpyxl |
| `.eml` | Email body + attachment extraction |
| `.txt`, `.rtf` | Direct text read |

## Cost Tracking

After execution, a summary shows:
- Total pages processed (text vs. mixed vs. image breakdown)
- Vision API calls saved by 3-tier classification
- Total input/output tokens
- Estimated cost (input @ $18/1M tokens, output @ $85/1M tokens)
- Per-file cost breakdown

## Error Handling

- If a page fails JSON parsing, it retries once with a corrective prompt
- Text page extraction retries twice before falling back to Vision API
- Failed pages are logged and listed in the Failed_Pages Excel tab
- Scalar gate timeout: 10 minutes before auto-cancellation
