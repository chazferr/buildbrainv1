"""
BuildBrain Extraction Engine
=============================
Shared extraction logic used by both the CLI (extract_to_excel.py)
and the web app (app.py).
"""

import base64
import csv
import email
import io
import json
import re
import time
from email import policy
from pathlib import Path
from typing import Callable, Optional

import anthropic
import fitz  # PyMuPDF
import pandas as pd
from pydantic import BaseModel, ValidationError, field_validator

# Optional imports for extended file types
try:
    from docx import Document as DocxDocument
except ImportError:
    DocxDocument = None

# ─── Constants ───────────────────────────────────────────────────────────────

MODEL = "claude-opus-4-6"
TEMPERATURE = 0
DPI = 200
MAX_IMAGE_BYTES = 4_800_000  # Stay under Claude's 5MB base64 limit

# Cost rates (USD per 1M tokens)
INPUT_RATE = 18.0
OUTPUT_RATE = 85.0

# ─── Pydantic schemas ───────────────────────────────────────────────────────


class SubmissionRequirement(BaseModel):
    category: str
    requirement: str
    mandatory: bool
    source_pdf: str
    source_page: int
    evidence: str

    @field_validator("source_page")
    @classmethod
    def page_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("source_page must be >= 1")
        return v


class TradeAndScope(BaseModel):
    csi_division: str
    trade: str
    scope_description: str
    estimated_cost: Optional[float] = None
    vendor_name: Optional[str] = None
    quantity: Optional[str] = None
    source_pdf: str
    source_page: int
    evidence: str

    @field_validator("source_page")
    @classmethod
    def page_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("source_page must be >= 1")
        return v


class PageExtraction(BaseModel):
    submission_requirements: list[SubmissionRequirement]
    trades_and_scope: list[TradeAndScope]


# ─── Prompts ─────────────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """\
You are a construction document analyst. You are looking at a single page from \
a construction PDF drawing set.

Your task: extract ONLY information that is clearly visible on THIS page.

Return a JSON object with exactly this structure (no markdown fences, no extra keys):
{{
  "submission_requirements": [
    {{
      "category": "<string: e.g. Permits, Submittals, Inspections, Insurance, Bonds, Certifications, etc.>",
      "requirement": "<string: concise description of the requirement>",
      "mandatory": <boolean: true if clearly mandatory, false if optional or unclear>,
      "source_pdf": "{pdf_name}",
      "source_page": {page_num},
      "evidence": "<string: short verbatim quote or paraphrase from the page proving this>"
    }}
  ],
  "trades_and_scope": [
    {{
      "csi_division": "<string: 2-digit CSI MasterFormat division if identifiable, else 'NA'>",
      "trade": "<string: trade name e.g. Earthwork, Concrete, Structural Steel, Plumbing, Electrical, HVAC, etc.>",
      "scope_description": "<string: brief description of scope visible on this page>",
      "estimated_cost": <number or null: dollar amount for this line item. Extract from bids, budgets, estimates, allowances, lump sums, unit prices x qty, contract amounts, SOV amounts, or any dollar figure tied to this trade. Strip $ and commas — e.g. $12,500 -> 12500. null if no dollar amount visible>,
      "vendor_name": "<string or null: name of the bidder, contractor, vendor, subcontractor, or supplier associated with this cost/trade — e.g. 'Eastern Concrete', 'L&R Mechanical', 'Budget'. null if no vendor/bidder name is visible>",
      "quantity": "<string or null: quantity with unit if visible, e.g. '2,400 SF', '150 LF', '12 EA'. null if none>",
      "source_pdf": "{pdf_name}",
      "source_page": {page_num},
      "evidence": "<string: short verbatim quote or visual description from the page proving this>"
    }}
  ]
}}

RULES:
- ONLY extract what is clearly visible. Do NOT hallucinate or infer beyond what is shown.
- If the page is a cover sheet, title block, or contains no extractable info, return empty arrays.
- For drawings: identify trades from drawing content (e.g., site plan implies Earthwork/Grading, \
  electrical symbols imply Electrical trade, plumbing fixtures imply Plumbing, etc.)
- For notes/specs: extract submission requirements and any trade/scope references.
- FINANCIAL DATA IS CRITICAL: This is a buyout/budgeting tool. Carefully scan every page for:
  * Dollar amounts ($), costs, prices, budgets, allowances, bids, estimates, contract values
  * Bid tabulations, cost breakdowns, SOV (Schedule of Values), line-item pricing
  * Unit prices (e.g., $4.50/SF) — multiply by quantity if both are shown
  * Lump-sum amounts, subtotals, per-trade totals
  * If a table or list shows trade names with dollar amounts, create one entry per trade with its cost
  Strip $ signs and commas — return a plain number (e.g. $12,500 -> 12500).
- VENDOR/BIDDER NAMES: If a company name, contractor, subcontractor, or supplier is associated \
  with a cost or trade, capture it in vendor_name. If the same trade has multiple vendors with \
  different bids, create SEPARATE entries for each vendor+amount pair.
- QUANTITIES: If quantities (SF, LF, EA, CY, SY, etc.) are visible, capture them in quantity.
- evidence must be a SHORT snippet (max ~80 chars) directly from the page.
- Return ONLY valid JSON. No markdown code fences. No commentary outside the JSON.
"""

RETRY_PROMPT = """\
The previous response was not valid JSON matching the required schema.

Return ONLY a valid JSON object with this exact structure:
{{
  "submission_requirements": [
    {{
      "category": "string",
      "requirement": "string",
      "mandatory": true/false,
      "source_pdf": "{pdf_name}",
      "source_page": {page_num},
      "evidence": "string"
    }}
  ],
  "trades_and_scope": [
    {{
      "csi_division": "string",
      "trade": "string",
      "scope_description": "string",
      "estimated_cost": null,
      "vendor_name": null,
      "quantity": null,
      "source_pdf": "{pdf_name}",
      "source_page": {page_num},
      "evidence": "string"
    }}
  ]
}}

If the page has no relevant content, return:
{{"submission_requirements": [], "trades_and_scope": []}}

ONLY output valid JSON. No markdown. No explanation.
"""


# ─── Helpers ─────────────────────────────────────────────────────────────────


def render_page_to_png(doc: fitz.Document, page_idx: int, dpi: int = DPI) -> bytes:
    """Render a single PDF page to PNG bytes at the given DPI.
    If the image exceeds MAX_IMAGE_BYTES, re-render at progressively lower DPI."""
    page = doc.load_page(page_idx)
    current_dpi = dpi
    while current_dpi >= 72:
        zoom = current_dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        png_bytes = pix.tobytes("png")
        if len(png_bytes) <= MAX_IMAGE_BYTES:
            return png_bytes
        current_dpi = int(current_dpi * 0.75)
    return png_bytes


def extract_json_from_text(text: str) -> str:
    """Try to extract JSON from response text, stripping markdown fences if present."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
        text = text.strip()
    return text


def normalize_key(s: str) -> str:
    """Normalize a string for dedup comparison."""
    return re.sub(r"\s+", " ", s.strip().lower())


def dedup_requirements(items: list[SubmissionRequirement]) -> list[SubmissionRequirement]:
    """Remove duplicate requirements based on normalized category+requirement."""
    seen = set()
    result = []
    for item in items:
        key = (normalize_key(item.category), normalize_key(item.requirement))
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


def dedup_trades(items: list[TradeAndScope]) -> list[TradeAndScope]:
    """Remove duplicate trades based on normalized trade+scope_description."""
    seen = set()
    result = []
    for item in items:
        key = (normalize_key(item.trade), normalize_key(item.scope_description))
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


# ─── Supported file types ──────────────────────────────────────────────────

SUPPORTED_EXTENSIONS = {
    # PDFs (image-based extraction)
    ".pdf",
    # Images (direct to Vision API)
    ".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".webp",
    # Documents (text extraction)
    ".docx", ".doc",
    # Spreadsheets (text extraction)
    ".xlsx", ".xls", ".csv",
    # Email (text extraction)
    ".eml",
    # Plain text
    ".txt", ".rtf",
}

# Max characters per text chunk sent to Claude
_TEXT_CHUNK_SIZE = 8000


def _extract_text_from_docx(file_path: Path) -> str:
    """Extract all text from a DOCX file."""
    if DocxDocument is None:
        raise ImportError("python-docx is required for DOCX files. Install with: pip install python-docx")
    doc = DocxDocument(str(file_path))
    paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
    # Also extract text from tables
    for table in doc.tables:
        for row in table.rows:
            cells = [cell.text.strip() for cell in row.cells if cell.text.strip()]
            if cells:
                paragraphs.append(" | ".join(cells))
    return "\n".join(paragraphs)


def _extract_text_from_eml(file_path: Path) -> str:
    """Extract text content from an EML email file."""
    with open(file_path, "rb") as f:
        msg = email.message_from_binary_file(f, policy=policy.default)

    parts = []
    # Headers
    for header in ["From", "To", "Subject", "Date", "CC"]:
        val = msg.get(header)
        if val:
            parts.append(f"{header}: {val}")

    parts.append("")  # blank line after headers

    # Body
    body = msg.get_body(preferencelist=("plain", "html"))
    if body:
        content = body.get_content()
        if isinstance(content, bytes):
            content = content.decode("utf-8", errors="replace")
        parts.append(content)

    # List attachments
    attachments = []
    for part in msg.walk():
        fn = part.get_filename()
        if fn:
            attachments.append(fn)
    if attachments:
        parts.append("\n--- Attachments ---")
        for att in attachments:
            parts.append(f"  - {att}")

    return "\n".join(parts)


def _extract_text_from_spreadsheet(file_path: Path) -> str:
    """Extract text content from XLSX, XLS, or CSV files."""
    ext = file_path.suffix.lower()
    parts = []

    if ext == ".csv":
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            for row_idx, row in enumerate(reader):
                if any(cell.strip() for cell in row):
                    parts.append(" | ".join(cell.strip() for cell in row))
    else:
        # XLSX / XLS via pandas
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                parts.append(f"--- Sheet: {sheet_name} ---")
                for _, row in df.iterrows():
                    cells = [str(v).strip() for v in row if pd.notna(v) and str(v).strip()]
                    if cells:
                        parts.append(" | ".join(cells))
                parts.append("")
        except Exception as e:
            parts.append(f"[Error reading spreadsheet: {e}]")

    return "\n".join(parts)


def _extract_text_from_txt(file_path: Path) -> str:
    """Read plain text or RTF file."""
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def _chunk_text(text: str, chunk_size: int = _TEXT_CHUNK_SIZE) -> list[str]:
    """Split text into chunks, breaking at line boundaries."""
    lines = text.split("\n")
    chunks = []
    current = []
    current_len = 0

    for line in lines:
        line_len = len(line) + 1
        if current_len + line_len > chunk_size and current:
            chunks.append("\n".join(current))
            current = []
            current_len = 0
        current.append(line)
        current_len += line_len

    if current:
        chunks.append("\n".join(current))

    return chunks if chunks else ["(empty document)"]


def _get_image_media_type(file_path: Path) -> str:
    """Get MIME type for image files."""
    ext = file_path.suffix.lower()
    return {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".tiff": "image/tiff",
        ".tif": "image/tiff",
        ".bmp": "image/bmp",
        ".webp": "image/webp",
    }.get(ext, "image/png")


# ─── Text-based page processing ───────────────────────────────────────────


def process_text_page(
    client: anthropic.Anthropic,
    text_content: str,
    file_name: str,
    page_num: int,
    stats: dict,
) -> Optional[PageExtraction]:
    """Send text content (from DOCX/EML/XLSX/etc.) to Claude for extraction."""

    prompt_text = EXTRACTION_PROMPT.format(pdf_name=file_name, page_num=page_num)

    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": f"--- DOCUMENT CONTENT ({file_name}, section {page_num}) ---\n\n"
                            f"{text_content}\n\n"
                            f"--- END DOCUMENT CONTENT ---\n\n"
                            f"{prompt_text}",
                },
            ],
        }
    ]

    for attempt in range(2):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                temperature=TEMPERATURE,
                messages=messages,
            )

            usage = response.usage
            stats["input_tokens"] += usage.input_tokens
            stats["output_tokens"] += usage.output_tokens
            stats["api_calls"] += 1

            raw_text = response.content[0].text
            json_text = extract_json_from_text(raw_text)

            data = json.loads(json_text)
            extraction = PageExtraction(**data)
            return extraction

        except (json.JSONDecodeError, ValidationError):
            if attempt == 0:
                retry_text = RETRY_PROMPT.format(pdf_name=file_name, page_num=page_num)
                messages.append({"role": "assistant", "content": raw_text})
                messages.append({"role": "user", "content": retry_text})
            else:
                stats["failed_pages"].append(f"{file_name} section {page_num}")
                return None

        except anthropic.APIError:
            if attempt == 0:
                time.sleep(5)
            else:
                stats["failed_pages"].append(f"{file_name} section {page_num}")
                return None

    return None


# ─── Trade consolidation ────────────────────────────────────────────────────

# Master trade list matching the buyout sheet format.
# Each entry: (sort_order, division, trade_name, keyword_patterns)
# keyword_patterns match against BOTH the raw trade name and the CSI division.
# First match wins, so more specific patterns go first.
_BUYOUT_TRADES: list[tuple[int, str, str, list[str]]] = [
    # Div 2 — Sitework
    (1,  "2",  "Sitework",               ["sitework", "earthwork", "grading", "excavat", "erosion",
                                           "sediment", "site drainage", "demolit", "abatement",
                                           "existing condition", "drainage", "storm drain",
                                           "catch basin", "stormwater", "utilities", "water util",
                                           "sewer util", "site improv", "exterior improv",
                                           "retaining wall", "erosion control"]),
    (2,  "2",  "Paving & Striping",      ["paving", "striping", "asphalt", "parking"]),
    (3,  "2",  "Landscaping",            ["landscape", "landscaping", "planting", "irrigation",
                                           "bioretention", "plant list"]),
    (4,  "2",  "Fencing",                ["fenc"]),
    # Div 3 — Concrete
    (5,  "3",  "Site Concrete",          ["site concrete", "sidewalk", "curb", "ramp",
                                           "concrete flatwork"]),
    (6,  "3",  "Building Concrete",      ["concrete", "foundation", "footing", "slab", "masonry"]),
    # Div 5 — Metals
    (7,  "5",  "Structural Steel",       ["structural", "steel", "metal connect", "simpson",
                                           "strap", "clip", "anchor bolt"]),
    # Div 6 — Wood & Plastics
    (8,  "6",  "Rough Carpentry",        ["rough carp", "wood fram", "framing", "stud", "joist",
                                           "rafter", "truss", "sheathing", "blocking",
                                           "roof framing", "attic framing", "wood/plastic",
                                           "composites", "pvc", "architectural",
                                           "equipment plan"]),
    (10, "8",  "Windows",                ["window", "glazing", "marvin"]),
    (9,  "8",  "Doors/Hdwr/Finish Carp", ["door", "hardware", "finish carp", "trim", "casework",
                                           "millwork", "woodwork", "opening",
                                           "hdwr", "lockset", "lever handle"]),
    # Div 7 — Thermal & Moisture
    (11, "7",  "Insulation",             ["insulation", "insul", "spray foam", "batt",
                                           "thermal", "moisture protect", "vapor barrier",
                                           "air barrier", "air infiltrat", "moisture bar",
                                           "housewrap", "tyvek", "hydrogap"]),
    (14, "5",  "Gutters",               ["gutter", "downspout"]),
    (15, "7",  "Flashing & Waterproofing", ["waterproof", "dampproof", "damp-proof",
                                           "flash", "sheet metal", "drip edge",
                                           "sealant", "caulk", "backer rod", "termite"]),
    (12, "7",  "Roofing",               ["roofing", "shingle", "roof assembly", "roof membran",
                                           "ice.*water", "underlayment", "roof felt"]),
    (13, "7",  "Siding",                ["siding", "exterior clad", "vinyl sid",
                                           "exterior enclos", "cedar impression"]),
    # Div 9 — Finishes
    (16, "9",  "Drywall",               ["drywall", "gypsum", "gwb", "resilient channel"]),
    (17, "9",  "Flooring",              ["floor", "lvt", "vinyl tile", "carpet", "tile",
                                           "ceramic", "non-slip"]),
    (18, "9",  "Painting",              ["paint", "coating", "primer", "finish schedule",
                                           "room finish"]),
    (19, "9",  "Countertops & Finishes", ["countertop", "hi-macs", "finish"]),
    # Div 10-13 — Specialties & Equipment
    (20, "10", "Specialties",            ["specialt", "accessori", "grab bar", "mirror",
                                           "towel bar", "shower control"]),
    # Div 14 — Conveying
    (22, "14", "Conveying Equipment",    ["elevator", "conveying", "lift", "ceiling lift"]),
    # Div 15 (22-23) — Mechanical (plumbing BEFORE cabinets so "Plumbing/Kitchen" matches here)
    (23, "15", "Plumbing",              ["plumb", "piping", "fixture", "toilet", "lavatory",
                                           "shower", "bathtub", "water heater", "vanity",
                                           "penguin", "overflow"]),
    (24, "15", "HVAC",                  ["hvac", "mechanical", "mini-split", "mitsubishi",
                                           "air condition", "heating", "duct", "ventilat",
                                           "continuous vent"]),
    # Cabinets after Plumbing so "Plumbing/Kitchen" doesn't match "kitchen" first
    (21, "13", "Cabinets",              ["cabinet", "kitchen", "furnish", "appliance",
                                           "express kitchen"]),
    # Div 16 (26-28) — Electrical
    (25, "16", "Electrical",            ["electric", "lighting", "outlet", "panel", "amp",
                                           "low voltage", "control panel", "site light",
                                           "luminaire", "wiring", "transformer"]),
    (26, "16", "Smart Home / Security",  ["communicat", "smart", "speaker", "network", "data",
                                           "safety", "security", "alarm", "arms", "fire protect",
                                           "camera", "doorbell", "sensor", "blind"]),
    # Div 1 — General (catch-all, placed last)
    (27, "1",  "General Requirements",   ["general", "project info", "coordination"]),
]


def _classify_trade(raw_trade: str, raw_csi: str) -> tuple[int, str, str]:
    """
    Classify a raw trade extraction into a buyout-style trade category.
    Returns (sort_order, division, canonical_trade_name).
    """
    lower = raw_trade.lower()

    for sort_order, div, canon, patterns in _BUYOUT_TRADES:
        for pat in patterns:
            if pat in lower:
                return (sort_order, div, canon)

    # Fallback: try to use the CSI division number to find a default bucket
    raw_num = str(raw_csi).strip().split(".")[0]
    try:
        num = int(float(raw_num))
    except (ValueError, TypeError):
        num = 0

    # Map extended CSI (22-33) to traditional divisions and find a default
    div_defaults = {
        1: (27, "1", "General Requirements"),
        2: (1, "2", "Sitework"),
        3: (6, "3", "Building Concrete"),
        4: (6, "3", "Building Concrete"),
        5: (7, "5", "Structural Steel"),
        6: (8, "6", "Rough Carpentry"),
        7: (11, "7", "Insulation"),
        8: (9, "8", "Doors/Hdwr/Finish Carp"),
        9: (16, "9", "Drywall"),
        10: (20, "10", "Specialties"),
        12: (21, "13", "Cabinets"),
        13: (21, "13", "Cabinets"),
        14: (22, "14", "Conveying Equipment"),
        22: (23, "15", "Plumbing"),
        23: (24, "15", "HVAC"),
        26: (25, "16", "Electrical"),
        27: (26, "16", "Smart Home / Security"),
        28: (26, "16", "Smart Home / Security"),
        31: (1, "2", "Sitework"),
        32: (2, "2", "Paving & Striping"),
        33: (1, "2", "Sitework"),
    }

    if num in div_defaults:
        return div_defaults[num]

    return (27, "1", "General Requirements")


def consolidate_trades(trades: list[TradeAndScope]) -> list[dict]:
    """
    Group raw per-page trade extractions into consolidated buyout-style rows:
    one row per canonical trade, with scope items listed in page order.

    Returns a list of dicts sorted by buyout order (matching the user's
    buyout sheet: Sitework, Paving, Landscaping, Concrete, ... Electrical).
    Each dict includes 'bids' — a list of (vendor_name, amount) pairs for
    populating the bid columns in the Excel output.
    """

    groups: dict[str, dict] = {}  # keyed by canonical trade name

    for t in trades:
        sort_order, div, canon = _classify_trade(t.trade, t.csi_division)
        key = canon

        if key not in groups:
            groups[key] = {
                "sort_order": sort_order,
                "csi_division": div,
                "trade": canon,
                "scope_items": [],       # (page_order_key, description, cost, qty)
                "sources": [],           # "PDF p.N"
                "seen_scope": set(),     # for dedup
                "total_cost": 0.0,       # sum of all extracted costs
                "has_cost": False,       # whether any cost was found
                "quantities": [],        # collected quantity strings
                "bids": {},              # vendor_name -> best (lowest) amount
                "unnamed_costs": [],     # costs with no vendor name
            }

        g = groups[key]

        # Dedup scope descriptions
        norm_scope = normalize_key(t.scope_description)
        if norm_scope not in g["seen_scope"]:
            g["seen_scope"].add(norm_scope)
            order_key = (t.source_pdf, t.source_page)
            g["scope_items"].append((order_key, t.scope_description,
                                     t.estimated_cost, t.quantity))

        # Collect bids: vendor + amount pairs
        if t.estimated_cost is not None and t.estimated_cost > 0:
            g["total_cost"] += t.estimated_cost
            g["has_cost"] = True

            if t.vendor_name and t.vendor_name.strip():
                vname = t.vendor_name.strip()
                # Keep the largest amount per vendor (they may bid on multiple scope items)
                if vname in g["bids"]:
                    g["bids"][vname] += t.estimated_cost
                else:
                    g["bids"][vname] = t.estimated_cost
            else:
                g["unnamed_costs"].append(t.estimated_cost)

        # Collect quantities
        if t.quantity:
            if t.quantity not in g["quantities"]:
                g["quantities"].append(t.quantity)

        source_ref = f"{t.source_pdf} p.{t.source_page}"
        if source_ref not in g["sources"]:
            g["sources"].append(source_ref)

    # Build consolidated rows sorted by buyout order
    result = []
    for key in sorted(groups.keys(), key=lambda k: groups[k]["sort_order"]):
        g = groups[key]
        # Sort scope items by page order (PDF name, then page number)
        g["scope_items"].sort(key=lambda x: x[0])
        # Build numbered scope text (include cost/qty inline when available)
        scope_lines = []
        for i, (_, desc, cost, qty) in enumerate(g["scope_items"], 1):
            parts = [f"{i}. {desc}"]
            if qty:
                parts.append(f"[{qty}]")
            if cost and cost > 0:
                parts.append(f"(${cost:,.0f})")
            scope_lines.append(" ".join(parts))
        scope_text = "\n".join(scope_lines)

        # Build bid list: sorted by amount (lowest first)
        bid_list = sorted(g["bids"].items(), key=lambda x: x[1])
        # If we have unnamed costs but no vendor bids, aggregate them as "Budget"
        if not bid_list and g["unnamed_costs"]:
            total_unnamed = sum(g["unnamed_costs"])
            bid_list = [("Budget", total_unnamed)]

        # Determine budget/SOV: use total_cost if any costs were found
        budget = g["total_cost"] if g["has_cost"] else None

        result.append({
            "csi_division": g["csi_division"],
            "trade": g["trade"],
            "scope": scope_text,
            "source_pages": "; ".join(g["sources"]),
            "budget": budget,
            "quantities": "; ".join(g["quantities"]) if g["quantities"] else None,
            "bids": bid_list,  # list of (vendor_name, amount) sorted low->high
        })

    return result


# ─── Page processing ────────────────────────────────────────────────────────


def process_page(
    client: anthropic.Anthropic,
    image_bytes: bytes,
    pdf_name: str,
    page_num: int,
    stats: dict,
) -> Optional[PageExtraction]:
    """Send a page image to Claude and parse the extraction result."""

    b64_image = base64.standard_b64encode(image_bytes).decode("utf-8")
    prompt_text = EXTRACTION_PROMPT.format(pdf_name=pdf_name, page_num=page_num)

    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": b64_image,
                    },
                },
                {
                    "type": "text",
                    "text": prompt_text,
                },
            ],
        }
    ]

    for attempt in range(2):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                temperature=TEMPERATURE,
                messages=messages,
            )

            usage = response.usage
            stats["input_tokens"] += usage.input_tokens
            stats["output_tokens"] += usage.output_tokens
            stats["api_calls"] += 1

            raw_text = response.content[0].text
            json_text = extract_json_from_text(raw_text)

            data = json.loads(json_text)
            extraction = PageExtraction(**data)
            return extraction

        except (json.JSONDecodeError, ValidationError) as e:
            if attempt == 0:
                retry_text = RETRY_PROMPT.format(pdf_name=pdf_name, page_num=page_num)
                messages = [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": "image/png",
                                    "data": b64_image,
                                },
                            },
                            {
                                "type": "text",
                                "text": prompt_text,
                            },
                        ],
                    },
                    {
                        "role": "assistant",
                        "content": raw_text,
                    },
                    {
                        "role": "user",
                        "content": retry_text,
                    },
                ]
            else:
                stats["failed_pages"].append(f"{pdf_name} p.{page_num}")
                return None

        except anthropic.APIError:
            if attempt == 0:
                time.sleep(5)
            else:
                stats["failed_pages"].append(f"{pdf_name} p.{page_num}")
                return None

    return None


# ─── Orchestrator ────────────────────────────────────────────────────────────


def _process_pdf(client, file_path, emit, global_stats, file_stats):
    """Process a PDF file page by page using Vision API."""
    file_name = file_path.name
    doc = fitz.open(str(file_path))
    num_pages = len(doc)
    emit(f"{file_name} has {num_pages} pages")

    per_file = {"input_tokens": 0, "output_tokens": 0, "api_calls": 0,
                "pages": num_pages, "failed_pages": []}
    reqs, trades = [], []

    for page_idx in range(num_pages):
        page_num = page_idx + 1
        emit(f"Processing {file_name} — Page {page_num}/{num_pages}...")

        try:
            png_bytes = render_page_to_png(doc, page_idx)
        except Exception as e:
            emit(f"[RENDER ERROR] {file_name} p.{page_num}: {e}")
            per_file["failed_pages"].append(f"{file_name} p.{page_num}")
            continue

        page_stats = {"input_tokens": 0, "output_tokens": 0, "api_calls": 0, "failed_pages": []}
        extraction = process_page(client, png_bytes, file_name, page_num, page_stats)

        per_file["input_tokens"] += page_stats["input_tokens"]
        per_file["output_tokens"] += page_stats["output_tokens"]
        per_file["api_calls"] += page_stats["api_calls"]
        per_file["failed_pages"].extend(page_stats["failed_pages"])

        if extraction:
            emit(f"{file_name} p.{page_num} OK (reqs={len(extraction.submission_requirements)}, trades={len(extraction.trades_and_scope)})")
            reqs.extend(extraction.submission_requirements)
            trades.extend(extraction.trades_and_scope)
        else:
            emit(f"{file_name} p.{page_num} SKIPPED")

    doc.close()
    file_stats[file_name] = per_file
    for k in ["input_tokens", "output_tokens", "api_calls"]:
        global_stats[k] += per_file[k]
    global_stats["total_pages"] += per_file["pages"]
    global_stats["failed_pages"].extend(per_file["failed_pages"])
    return reqs, trades


def _process_image(client, file_path, emit, global_stats, file_stats):
    """Process a standalone image file via Vision API."""
    file_name = file_path.name
    emit(f"Processing image {file_name}...")

    image_bytes = file_path.read_bytes()
    if len(image_bytes) > MAX_IMAGE_BYTES:
        emit(f"[WARNING] {file_name} is large ({len(image_bytes)} bytes), may be truncated")

    media_type = _get_image_media_type(file_path)
    per_file = {"input_tokens": 0, "output_tokens": 0, "api_calls": 0,
                "pages": 1, "failed_pages": []}

    # Use the same process_page but with the raw image bytes
    page_stats = {"input_tokens": 0, "output_tokens": 0, "api_calls": 0, "failed_pages": []}

    b64_image = base64.standard_b64encode(image_bytes).decode("utf-8")
    prompt_text = EXTRACTION_PROMPT.format(pdf_name=file_name, page_num=1)
    messages = [{"role": "user", "content": [
        {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64_image}},
        {"type": "text", "text": prompt_text},
    ]}]

    extraction = None
    for attempt in range(2):
        try:
            response = client.messages.create(model=MODEL, max_tokens=4096,
                                              temperature=TEMPERATURE, messages=messages)
            page_stats["input_tokens"] += response.usage.input_tokens
            page_stats["output_tokens"] += response.usage.output_tokens
            page_stats["api_calls"] += 1
            raw_text = response.content[0].text
            data = json.loads(extract_json_from_text(raw_text))
            extraction = PageExtraction(**data)
            break
        except (json.JSONDecodeError, ValidationError):
            if attempt == 0:
                messages.append({"role": "assistant", "content": raw_text})
                messages.append({"role": "user", "content": RETRY_PROMPT.format(pdf_name=file_name, page_num=1)})
            else:
                page_stats["failed_pages"].append(file_name)
        except anthropic.APIError:
            if attempt == 0:
                time.sleep(5)
            else:
                page_stats["failed_pages"].append(file_name)

    per_file["input_tokens"] = page_stats["input_tokens"]
    per_file["output_tokens"] = page_stats["output_tokens"]
    per_file["api_calls"] = page_stats["api_calls"]
    per_file["failed_pages"] = page_stats["failed_pages"]
    file_stats[file_name] = per_file
    for k in ["input_tokens", "output_tokens", "api_calls"]:
        global_stats[k] += per_file[k]
    global_stats["total_pages"] += 1
    global_stats["failed_pages"].extend(per_file["failed_pages"])

    reqs, trades = [], []
    if extraction:
        emit(f"{file_name} OK (reqs={len(extraction.submission_requirements)}, trades={len(extraction.trades_and_scope)})")
        reqs = list(extraction.submission_requirements)
        trades = list(extraction.trades_and_scope)
    else:
        emit(f"{file_name} SKIPPED")
    return reqs, trades


def _process_text_file(client, file_path, emit, global_stats, file_stats):
    """Process a text-based file (DOCX, EML, XLSX, CSV, TXT) by extracting text and sending to Claude."""
    file_name = file_path.name
    ext = file_path.suffix.lower()

    emit(f"Extracting text from {file_name}...")

    try:
        if ext == ".docx":
            text = _extract_text_from_docx(file_path)
        elif ext == ".eml":
            text = _extract_text_from_eml(file_path)
        elif ext in (".xlsx", ".xls", ".csv"):
            text = _extract_text_from_spreadsheet(file_path)
        elif ext in (".txt", ".rtf", ".doc"):
            text = _extract_text_from_txt(file_path)
        else:
            text = _extract_text_from_txt(file_path)
    except Exception as e:
        emit(f"[ERROR] Could not read {file_name}: {e}")
        return [], []

    if not text.strip():
        emit(f"[WARNING] {file_name} has no extractable text, skipping")
        return [], []

    # Chunk the text and process each chunk
    chunks = _chunk_text(text)
    num_chunks = len(chunks)
    emit(f"{file_name}: {len(text)} chars, {num_chunks} section(s)")

    per_file = {"input_tokens": 0, "output_tokens": 0, "api_calls": 0,
                "pages": num_chunks, "failed_pages": []}
    reqs, trades = [], []

    for chunk_idx, chunk in enumerate(chunks):
        section_num = chunk_idx + 1
        emit(f"Processing {file_name} — Section {section_num}/{num_chunks}...")

        page_stats = {"input_tokens": 0, "output_tokens": 0, "api_calls": 0, "failed_pages": []}
        extraction = process_text_page(client, chunk, file_name, section_num, page_stats)

        per_file["input_tokens"] += page_stats["input_tokens"]
        per_file["output_tokens"] += page_stats["output_tokens"]
        per_file["api_calls"] += page_stats["api_calls"]
        per_file["failed_pages"].extend(page_stats["failed_pages"])

        if extraction:
            emit(f"{file_name} s.{section_num} OK (reqs={len(extraction.submission_requirements)}, trades={len(extraction.trades_and_scope)})")
            reqs.extend(extraction.submission_requirements)
            trades.extend(extraction.trades_and_scope)
        else:
            emit(f"{file_name} s.{section_num} SKIPPED")

    file_stats[file_name] = per_file
    for k in ["input_tokens", "output_tokens", "api_calls"]:
        global_stats[k] += per_file[k]
    global_stats["total_pages"] += per_file["pages"]
    global_stats["failed_pages"].extend(per_file["failed_pages"])
    return reqs, trades


def process_files(
    file_paths: list[Path],
    api_key: str,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> tuple[list[SubmissionRequirement], list[TradeAndScope], dict]:
    """
    Process a list of files (PDFs, images, DOCX, EML, XLSX, CSV, TXT):
    extract requirements and trades from every file.

    Args:
        file_paths: List of Path objects pointing to supported files.
        api_key: Anthropic API key.
        progress_callback: Optional callable that receives progress message strings.

    Returns:
        (requirements, trades, stats) where stats is a dict with token counts,
        costs, page counts, and per-file breakdowns.
    """
    def emit(msg: str):
        if progress_callback:
            progress_callback(msg)

    client = anthropic.Anthropic(api_key=api_key)

    all_requirements: list[SubmissionRequirement] = []
    all_trades: list[TradeAndScope] = []

    file_stats: dict[str, dict] = {}
    global_stats = {
        "total_pages": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "api_calls": 0,
        "failed_pages": [],
    }

    image_exts = {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".webp"}
    text_exts = {".docx", ".doc", ".xlsx", ".xls", ".csv", ".eml", ".txt", ".rtf"}

    for file_path in file_paths:
        if not file_path.exists():
            emit(f"[WARNING] File not found, skipping: {file_path}")
            continue

        ext = file_path.suffix.lower()
        emit(f"Opening {file_path.name}...")

        if ext == ".pdf":
            reqs, trades = _process_pdf(client, file_path, emit, global_stats, file_stats)
        elif ext in image_exts:
            reqs, trades = _process_image(client, file_path, emit, global_stats, file_stats)
        elif ext in text_exts:
            reqs, trades = _process_text_file(client, file_path, emit, global_stats, file_stats)
        else:
            emit(f"[WARNING] Unsupported file type: {ext}, skipping {file_path.name}")
            continue

        all_requirements.extend(reqs)
        all_trades.extend(trades)

    # Dedup
    emit("Deduplicating results...")
    all_requirements = dedup_requirements(all_requirements)
    all_trades = dedup_trades(all_trades)

    # Compute costs
    input_cost = (global_stats["input_tokens"] / 1_000_000) * INPUT_RATE
    output_cost = (global_stats["output_tokens"] / 1_000_000) * OUTPUT_RATE
    total_cost = input_cost + output_cost

    consolidated = consolidate_trades(all_trades)

    stats = {
        **global_stats,
        "input_cost": input_cost,
        "output_cost": output_cost,
        "total_cost": total_cost,
        "file_stats": file_stats,
        "num_requirements": len(all_requirements),
        "num_trades": len(consolidated),
        "num_raw_trade_items": len(all_trades),
    }

    emit(f"Done! {len(all_requirements)} requirements, {len(consolidated)} trades extracted.")

    return all_requirements, all_trades, stats


# Backwards compatibility alias
process_pdfs = process_files


# ─── Excel builder ───────────────────────────────────────────────────────────


def build_excel_bytes(
    requirements: list[SubmissionRequirement],
    trades: list[TradeAndScope],
) -> bytes:
    """Build the Excel workbook in memory and return it as bytes.

    Produces:
      Tab 1 — Buyout Summary    (buyout-style with financials)
      Tab 2 — Scope Details     (consolidated scope per trade)
      Tab 3 — Submission Reqs   (permits, submittals, inspections)
      Tab 4 — Source Trace      (raw per-page evidence)
    """
    import openpyxl
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side, numbers
    from openpyxl.utils import get_column_letter

    consolidated = consolidate_trades(trades)

    wb = openpyxl.Workbook()

    # ── Styles ────────────────────────────────────────────────────────────
    navy_fill = PatternFill(start_color="1A2332", end_color="1A2332", fill_type="solid")
    orange_fill = PatternFill(start_color="E8722A", end_color="E8722A", fill_type="solid")
    light_gray_fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
    summary_fill = PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid")

    header_font = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
    title_font = Font(name="Calibri", bold=True, color="FFFFFF", size=14)
    bold_font = Font(name="Calibri", bold=True, size=11)
    normal_font = Font(name="Calibri", size=11)
    money_fmt = '#,##0'
    thin_border = Border(
        left=Side(style="thin", color="CCCCCC"),
        right=Side(style="thin", color="CCCCCC"),
        top=Side(style="thin", color="CCCCCC"),
        bottom=Side(style="thin", color="CCCCCC"),
    )
    top_align = Alignment(vertical="top")
    wrap_top = Alignment(vertical="top", wrap_text=True)

    # ── TAB 1: Buyout Summary ─────────────────────────────────────────────
    ws = wb.active
    ws.title = "Buyout Summary"

    # Column layout:
    # A=DIV  B=TRADE  C=SCOPE(brief)  D=Bid1  E=Bid1$  F=Bid2  G=Bid2$
    # H=Bid3  I=Bid3$  J=Bid4  K=Bid4$  L=Low Bidder  M=Low Bid
    # N=Budget/SOV  O=Variance  P=Notes
    col_headers = [
        ("A", "DIV", 7),
        ("B", "TRADE", 34),
        ("C", "SCOPE", 62),
        ("D", "Bid 1", 28),
        ("E", "$", 16),
        ("F", "Bid 2", 28),
        ("G", "$", 16),
        ("H", "Bid 3", 28),
        ("I", "$", 16),
        ("J", "Bid 4", 28),
        ("K", "$", 16),
        ("L", "LOW BIDDER", 28),
        ("M", "LOW BID", 16),
        ("N", "BUDGET / SOV", 18),
        ("O", "VARIANCE", 16),
        ("P", "NOTES", 36),
    ]

    # Row 1: title bar
    ws.merge_cells("A1:P1")
    title_cell = ws["A1"]
    title_cell.value = "BuildBrain Buyout Summary"
    title_cell.font = title_font
    title_cell.fill = navy_fill
    title_cell.alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[1].height = 40

    # Row 2: bid section header
    ws.merge_cells("D2:K2")
    bid_header = ws["D2"]
    bid_header.value = "Bids"
    bid_header.font = header_font
    bid_header.fill = orange_fill
    bid_header.alignment = Alignment(horizontal="center")
    for col_letter in ["L", "M", "N", "O", "P"]:
        c = ws[f"{col_letter}2"]
        c.fill = orange_fill
    ws.row_dimensions[2].height = 26

    # Row 3: column headers
    for col_letter, label, width in col_headers:
        cell = ws[f"{col_letter}3"]
        cell.value = label
        cell.font = header_font
        cell.fill = navy_fill
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = thin_border
        ws.column_dimensions[col_letter].width = width
    ws.row_dimensions[3].height = 30

    # Data rows start at row 4
    data_start = 4
    for i, row_data in enumerate(consolidated):
        r = data_start + i
        ws.cell(row=r, column=1, value=int(row_data["csi_division"])).font = bold_font
        ws.cell(row=r, column=2, value=row_data["trade"]).font = bold_font

        # Brief scope: first 3 items, truncated
        scope_lines = row_data["scope"].split("\n")
        brief = "; ".join(line.split(". ", 1)[-1] for line in scope_lines[:3])
        if len(scope_lines) > 3:
            brief += f" (+{len(scope_lines) - 3} more)"
        ws.cell(row=r, column=3, value=brief).alignment = wrap_top

        # Populate bid columns D-K from extracted bids
        # bids is a list of (vendor_name, amount) sorted low->high
        bids = row_data.get("bids", [])
        bid_slots = [(4, 5), (6, 7), (8, 9), (10, 11)]  # (name_col, amount_col) pairs
        for slot_idx, (name_col, amt_col) in enumerate(bid_slots):
            if slot_idx < len(bids):
                vendor, amount = bids[slot_idx]
                ws.cell(row=r, column=name_col, value=vendor).font = normal_font
                ws.cell(row=r, column=amt_col, value=amount).font = normal_font
            ws.cell(row=r, column=name_col).border = thin_border
            ws.cell(row=r, column=amt_col).border = thin_border

        # Money columns get number format
        for col in [5, 7, 9, 11, 13, 14, 15]:
            ws.cell(row=r, column=col).number_format = money_fmt

        # Low Bidder / Low Bid (L, M): auto-populate from lowest bid
        if bids:
            low_vendor, low_amount = bids[0]  # already sorted low->high
            ws.cell(row=r, column=12, value=low_vendor).font = bold_font
            ws.cell(row=r, column=13, value=low_amount).font = bold_font
        ws.cell(row=r, column=12).border = thin_border
        ws.cell(row=r, column=13).border = thin_border

        # Budget/SOV (N): populate from extracted costs if available
        budget_cell = ws.cell(row=r, column=14)
        if row_data.get("budget") is not None:
            budget_cell.value = row_data["budget"]
        budget_cell.border = thin_border

        # Variance formula (O): =M{r}-N{r}
        var_cell = ws.cell(row=r, column=15)
        var_cell.value = f"=M{r}-N{r}"
        var_cell.number_format = money_fmt
        var_cell.border = thin_border

        # Notes (P): empty
        ws.cell(row=r, column=16).border = thin_border

        # Alternate row shading
        if i % 2 == 1:
            for col in range(1, 17):
                ws.cell(row=r, column=col).fill = light_gray_fill

        # All cells get border
        for col in range(1, 17):
            ws.cell(row=r, column=col).border = thin_border

        ws.row_dimensions[r].height = max(30, min(len(scope_lines) * 18, 90))

    data_end = data_start + len(consolidated) - 1

    # ── Summary rows below data ──────────────────────────────────────────
    gap = data_end + 2

    # SUBTOTALS row
    ws.cell(row=gap, column=2, value="SUBTOTALS").font = bold_font
    ws[f"B{gap}"].fill = summary_fill
    for col in [5, 7, 9, 11, 13, 14, 15]:
        col_letter = get_column_letter(col)
        cell = ws.cell(row=gap, column=col)
        cell.value = f"=SUM({col_letter}{data_start}:{col_letter}{data_end})"
        cell.number_format = money_fmt
        cell.font = bold_font
        cell.fill = summary_fill
        cell.border = thin_border
    for col in range(1, 17):
        ws.cell(row=gap, column=col).fill = summary_fill
        ws.cell(row=gap, column=col).border = thin_border

    # GC OH&P row
    ohp_row = gap + 1
    ws.cell(row=ohp_row, column=1, value=1).font = normal_font
    ws.cell(row=ohp_row, column=2, value="GC OH&P").font = bold_font
    ws.cell(row=ohp_row, column=14).number_format = money_fmt
    ws.cell(row=ohp_row, column=14).border = thin_border
    ws.cell(row=ohp_row, column=16, value="Enter OH&P % or amount").font = Font(
        name="Calibri", size=10, italic=True, color="999999")
    for col in range(1, 17):
        ws.cell(row=ohp_row, column=col).border = thin_border

    # Bond Premium row
    bond_row = ohp_row + 1
    ws.cell(row=bond_row, column=1, value=1).font = normal_font
    ws.cell(row=bond_row, column=2, value="Bond Premium").font = bold_font
    ws.cell(row=bond_row, column=14).number_format = money_fmt
    ws.cell(row=bond_row, column=14).border = thin_border
    for col in range(1, 17):
        ws.cell(row=bond_row, column=col).border = thin_border

    # Permit row
    permit_row = bond_row + 1
    ws.cell(row=permit_row, column=1, value=1).font = normal_font
    ws.cell(row=permit_row, column=2, value="Permit Fees").font = bold_font
    ws.cell(row=permit_row, column=14).number_format = money_fmt
    ws.cell(row=permit_row, column=14).border = thin_border
    for col in range(1, 17):
        ws.cell(row=permit_row, column=col).border = thin_border

    # TOTAL row
    total_row = permit_row + 1
    ws.cell(row=total_row, column=2, value="TOTAL").font = Font(
        name="Calibri", bold=True, size=12)
    for col_idx in [14]:
        col_letter = get_column_letter(col_idx)
        cell = ws.cell(row=total_row, column=col_idx)
        cell.value = f"=SUM({col_letter}{gap}:{col_letter}{permit_row})"
        cell.number_format = money_fmt
        cell.font = Font(name="Calibri", bold=True, size=12)
    for col_idx in [13]:
        col_letter = get_column_letter(col_idx)
        cell = ws.cell(row=total_row, column=col_idx)
        cell.value = f"=SUM({col_letter}{gap}:{col_letter}{permit_row})"
        cell.number_format = money_fmt
        cell.font = Font(name="Calibri", bold=True, size=12)
    total_var = ws.cell(row=total_row, column=15)
    total_var.value = f"=M{total_row}-N{total_row}"
    total_var.number_format = money_fmt
    total_var.font = Font(name="Calibri", bold=True, size=12)
    for col in range(1, 17):
        ws.cell(row=total_row, column=col).fill = navy_fill
        ws.cell(row=total_row, column=col).font = Font(
            name="Calibri", bold=True, size=12, color="FFFFFF")
        ws.cell(row=total_row, column=col).border = thin_border

    # Freeze panes
    ws.freeze_panes = "D4"
    ws.sheet_properties.tabColor = "1A2332"

    # ── TAB 2: Scope Details ──────────────────────────────────────────────
    ws2 = wb.create_sheet("Scope Details")
    ws2.sheet_properties.tabColor = "E8722A"

    scope_headers = ["DIV", "TRADE", "SCOPE", "QUANTITIES", "BUDGET", "SOURCE PAGES"]
    for ci, h in enumerate(scope_headers, 1):
        cell = ws2.cell(row=1, column=ci, value=h)
        cell.font = header_font
        cell.fill = navy_fill
        cell.border = thin_border

    ws2.column_dimensions["A"].width = 7
    ws2.column_dimensions["B"].width = 34
    ws2.column_dimensions["C"].width = 90
    ws2.column_dimensions["D"].width = 30
    ws2.column_dimensions["E"].width = 18
    ws2.column_dimensions["F"].width = 50
    ws2.row_dimensions[1].height = 28

    for i, row_data in enumerate(consolidated):
        r = i + 2
        ws2.cell(row=r, column=1, value=int(row_data["csi_division"])).alignment = top_align
        ws2.cell(row=r, column=2, value=row_data["trade"]).alignment = top_align
        ws2.cell(row=r, column=2).font = bold_font
        scope_cell = ws2.cell(row=r, column=3, value=row_data["scope"])
        scope_cell.alignment = wrap_top
        qty_cell = ws2.cell(row=r, column=4, value=row_data.get("quantities") or "")
        qty_cell.alignment = wrap_top
        budget_cell = ws2.cell(row=r, column=5)
        if row_data.get("budget") is not None:
            budget_cell.value = row_data["budget"]
            budget_cell.number_format = money_fmt
        budget_cell.alignment = top_align
        ws2.cell(row=r, column=6, value=row_data["source_pages"]).alignment = wrap_top
        line_count = row_data["scope"].count("\n") + 1
        ws2.row_dimensions[r].height = max(30, line_count * 18)
        for ci in range(1, 7):
            ws2.cell(row=r, column=ci).border = thin_border

    # ── TAB 3: Submission Requirements ────────────────────────────────────
    ws3 = wb.create_sheet("Submission Requirements")
    ws3.sheet_properties.tabColor = "27AE60"

    req_headers = ["CATEGORY", "REQUIREMENT", "MANDATORY", "SOURCE PDF", "PAGE", "EVIDENCE"]
    req_widths = [22, 70, 14, 36, 8, 70]
    for ci, (h, w) in enumerate(zip(req_headers, req_widths), 1):
        cell = ws3.cell(row=1, column=ci, value=h)
        cell.font = header_font
        cell.fill = navy_fill
        cell.border = thin_border
        ws3.column_dimensions[get_column_letter(ci)].width = w
    ws3.row_dimensions[1].height = 28

    sorted_reqs = sorted(requirements, key=lambda r: (r.category, r.source_pdf, r.source_page))
    for i, req in enumerate(sorted_reqs):
        r = i + 2
        ws3.cell(row=r, column=1, value=req.category).border = thin_border
        ws3.cell(row=r, column=1).alignment = top_align
        ws3.cell(row=r, column=2, value=req.requirement).border = thin_border
        ws3.cell(row=r, column=2).alignment = wrap_top
        ws3.cell(row=r, column=3, value="Yes" if req.mandatory else "No").border = thin_border
        ws3.cell(row=r, column=3).alignment = Alignment(horizontal="center", vertical="top")
        ws3.cell(row=r, column=4, value=req.source_pdf).border = thin_border
        ws3.cell(row=r, column=4).alignment = wrap_top
        ws3.cell(row=r, column=5, value=req.source_page).border = thin_border
        ws3.cell(row=r, column=5).alignment = Alignment(horizontal="center", vertical="top")
        ws3.cell(row=r, column=6, value=req.evidence).border = thin_border
        ws3.cell(row=r, column=6).alignment = wrap_top
        # Row height based on longest text in requirement or evidence
        max_len = max(len(req.requirement), len(req.evidence))
        ws3.row_dimensions[r].height = max(28, min(int(max_len / 60) * 18 + 28, 80))

    # ── TAB 4: Source Trace ───────────────────────────────────────────────
    ws4 = wb.create_sheet("Source Trace")
    ws4.sheet_properties.tabColor = "7F8C9B"

    trace_headers = ["TYPE", "CATEGORY / TRADE", "DETAIL", "SOURCE PDF", "PAGE", "EVIDENCE"]
    trace_widths = [16, 34, 70, 36, 8, 70]
    for ci, (h, w) in enumerate(zip(trace_headers, trace_widths), 1):
        cell = ws4.cell(row=1, column=ci, value=h)
        cell.font = header_font
        cell.fill = navy_fill
        cell.border = thin_border
        ws4.column_dimensions[get_column_letter(ci)].width = w
    ws4.row_dimensions[1].height = 28

    all_traces = []
    for req in requirements:
        all_traces.append(("Requirement", req.category, req.requirement,
                           req.source_pdf, req.source_page, req.evidence))
    for t in trades:
        all_traces.append(("Trade", t.trade, t.scope_description,
                           t.source_pdf, t.source_page, t.evidence))
    all_traces.sort(key=lambda x: (x[3], x[4], x[0]))

    for i, (rtype, cat, detail, pdf, page, ev) in enumerate(all_traces):
        r = i + 2
        ws4.cell(row=r, column=1, value=rtype).border = thin_border
        ws4.cell(row=r, column=1).alignment = top_align
        ws4.cell(row=r, column=2, value=cat).border = thin_border
        ws4.cell(row=r, column=2).alignment = top_align
        ws4.cell(row=r, column=3, value=detail).border = thin_border
        ws4.cell(row=r, column=3).alignment = wrap_top
        ws4.cell(row=r, column=4, value=pdf).border = thin_border
        ws4.cell(row=r, column=4).alignment = wrap_top
        ws4.cell(row=r, column=5, value=page).border = thin_border
        ws4.cell(row=r, column=5).alignment = Alignment(horizontal="center", vertical="top")
        ws4.cell(row=r, column=6, value=ev).border = thin_border
        ws4.cell(row=r, column=6).alignment = wrap_top
        max_len = max(len(detail), len(ev))
        ws4.row_dimensions[r].height = max(26, min(int(max_len / 55) * 18 + 26, 70))

    # ── Save ──────────────────────────────────────────────────────────────
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
