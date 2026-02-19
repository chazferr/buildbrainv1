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

from wage_rates import get_wage, CT_WAGE_RATES
from material_db import get_material_price, MATERIAL_DB
from productivity_rates import calculate_labor, PRODUCTIVITY_RATES

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
- SUBMISSION REQUIREMENTS — only extract items that meet at least ONE of:
  * Has a specific deadline, date, or schedule constraint
  * Involves money, retainage, bonds, or insurance amounts
  * Is a license, certification, permit, or regulatory approval requirement
  * Creates legal or schedule risk if missed (liquidated damages, inspections with teeth)
  * Is explicitly marked mandatory, "shall", "required", or "must"
  * Involves coordination with an outside authority (city, DOT, utility, engineer of record)
  Do NOT extract: general best practices, standard boilerplate phrases like "contractor
  shall verify all dimensions", informational notes with no legal consequence, or items
  that are repeated verbatim from another page already processed.
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


# ─── SOV mapping from reference buyout ──────────────────────────────────────


def parse_sov_from_buyout(file_path: Path) -> dict:
    """
    Parse SOV (Schedule of Values) data from a reference buyout spreadsheet.

    Searches for an 'SOV' column header, then reads trade name -> SOV value pairs.
    Also extracts summary values (GC OH&P, Bond Premium, Permit Fees).

    Returns a dict with:
        'trade_sov': dict[str, float] - reference trade name -> SOV value
        'ohp': float or None - GC OH&P value
        'bond': float or None - Bond Premium value
        'permit': float or None - Permit Fees value
    """
    df_dict = pd.read_excel(file_path, sheet_name=None, header=None)

    result = {"trade_sov": {}, "ohp": None, "bond": None, "permit": None}

    for sheet_name, df in df_dict.items():
        # Search for ALL "SOV" column headers, use the rightmost (most recent)
        sov_positions = []
        for col_idx in range(len(df.columns)):
            for row_idx in range(min(15, len(df))):
                val = df.iloc[row_idx, col_idx]
                if pd.notna(val) and isinstance(val, str) and "sov" in val.lower():
                    sov_positions.append((row_idx, col_idx))
                    break

        if not sov_positions:
            continue

        sov_header_row, sov_col = max(sov_positions, key=lambda x: x[1])

        # Find the trade name column: look for columns with trade-like names
        trade_col = None
        trade_keywords = [
            "site", "pav", "concrete", "carp", "insul", "roof", "sid",
            "door", "window", "drywall", "floor", "paint", "plumb",
            "hvac", "electric", "cabinet", "gutter", "landscape", "fenc",
            "steel", "masonry", "framing",
        ]
        best_count = 0
        for col_idx in range(min(10, len(df.columns))):
            trade_count = 0
            for row_idx in range(sov_header_row + 1, min(len(df), sov_header_row + 30)):
                val = df.iloc[row_idx, col_idx]
                if pd.notna(val) and isinstance(val, str) and len(val.strip()) > 2:
                    lower = val.lower().strip()
                    if any(kw in lower for kw in trade_keywords):
                        trade_count += 1
            if trade_count > best_count:
                best_count = trade_count
                trade_col = col_idx

        if trade_col is None:
            trade_col = min(1, len(df.columns) - 1)

        # Extract trade -> SOV pairs
        for row_idx in range(sov_header_row + 1, len(df)):
            trade_val = df.iloc[row_idx, trade_col]
            sov_val = df.iloc[row_idx, sov_col]

            if pd.isna(trade_val) or pd.isna(sov_val):
                continue

            trade_name = str(trade_val).strip()
            if not trade_name:
                continue

            try:
                sov_num = float(sov_val)
            except (ValueError, TypeError):
                continue

            if sov_num == 0:
                continue

            # Detect summary rows
            lower_trade = trade_name.lower()
            if "oh&p" in lower_trade or "overhead" in lower_trade:
                result["ohp"] = sov_num
            elif "bond" in lower_trade:
                result["bond"] = sov_num
            elif "permit" in lower_trade:
                result["permit"] = sov_num
            elif "total" in lower_trade or "contract" in lower_trade:
                pass  # Skip total/subtotal rows
            else:
                result["trade_sov"][trade_name] = sov_num

        # Only process the first sheet with SOV data
        if result["trade_sov"]:
            break

    return result


def _match_sov_to_trades(
    consolidated: list[dict], sov_mapping: dict[str, float]
) -> dict[str, float]:
    """
    Match reference buyout trade names to BuildBrain canonical trade names.

    Returns a dict of canonical_trade_name -> SOV value.
    Uses three-pass matching: exact, containment, keyword classification.
    """
    matched = {}
    used_refs = set()

    # Pass 1: Exact match (case-insensitive)
    for row in consolidated:
        canon = row["trade"]
        canon_lower = canon.lower().strip()
        for ref_trade, sov_val in sov_mapping.items():
            if ref_trade in used_refs:
                continue
            if ref_trade.lower().strip() == canon_lower:
                matched[canon] = sov_val
                used_refs.add(ref_trade)
                break

    # Pass 2: Containment match
    for row in consolidated:
        canon = row["trade"]
        if canon in matched:
            continue
        canon_lower = canon.lower().strip()
        for ref_trade, sov_val in sov_mapping.items():
            if ref_trade in used_refs:
                continue
            ref_lower = ref_trade.lower().strip()
            if canon_lower in ref_lower or ref_lower in canon_lower:
                matched[canon] = sov_val
                used_refs.add(ref_trade)
                break

    # Pass 3: Classify reference trade using same keyword patterns
    for row in consolidated:
        canon = row["trade"]
        if canon in matched:
            continue
        for ref_trade, sov_val in sov_mapping.items():
            if ref_trade in used_refs:
                continue
            _, _, ref_canonical = _classify_trade(ref_trade, "")
            if ref_canonical == canon:
                matched[canon] = sov_val
                used_refs.add(ref_trade)
                break

    return matched


# ─── Pricing engine ──────────────────────────────────────────────────────────


def calculate_trade_estimate(
    trade_name: str,
    work_items: list[dict],
    wage_regime: str = "CT_DOL_RESIDENTIAL",
    sub_quote: float = None,
    sub_quote_source: str = None
) -> dict:
    """
    Calculates a complete trade estimate with full calculation trace.

    work_items is a list of dicts, each with:
        - "work_item": key from PRODUCTIVITY_RATES
        - "quantity": numeric quantity
        - "material_key": key from MATERIAL_DB (optional)
        - "material_quantity": quantity of material (optional, defaults to same as work quantity)
        - "description": human-readable description

    If sub_quote is provided, it OVERRIDES the calculated baseline.
    The calculated baseline is still shown for comparison.

    Returns a dict with full trace suitable for Excel SOV output.
    """
    line_items = []
    total_material = 0.0
    total_labor = 0.0

    for item in work_items:
        work_item_key = item.get("work_item")
        quantity = item.get("quantity", 0)
        material_key = item.get("material_key")
        mat_qty = item.get("material_quantity", quantity)
        description = item.get("description", work_item_key)

        # Labor calculation
        labor_result = None
        if work_item_key and quantity > 0:
            labor_result = calculate_labor(work_item_key, quantity, wage_regime)

        # Material calculation
        material_result = None
        if material_key and mat_qty > 0:
            mat_data = get_material_price(material_key)
            if mat_data:
                mat_cost = mat_qty * mat_data["price"]
                material_result = {
                    "key": material_key,
                    "quantity": mat_qty,
                    "unit": mat_data["unit"],
                    "unit_price": mat_data["price"],
                    "cost": mat_cost,
                    "note": mat_data["note"],
                    "trace": f"{mat_qty} {mat_data['unit']} \u00d7 ${mat_data['price']:.2f}/{mat_data['unit']} = ${mat_cost:,.0f}"
                }
                total_material += mat_cost

        labor_cost = labor_result["labor_cost"] if labor_result else 0
        total_labor += labor_cost

        line_items.append({
            "description": description,
            "labor": labor_result,
            "material": material_result,
            "line_total": labor_cost + (material_result["cost"] if material_result else 0)
        })

    calculated_total = total_material + total_labor

    # Sub quote override logic
    if sub_quote is not None:
        variance = sub_quote - calculated_total
        variance_pct = (variance / calculated_total * 100) if calculated_total > 0 else 0
        use_amount = sub_quote
        override_flag = True
        override_note = f"SUB QUOTE USED: ${sub_quote:,.0f} from {sub_quote_source or 'unnamed source'}. Baseline was ${calculated_total:,.0f} (variance: {variance_pct:+.1f}%)"
        if abs(variance_pct) > 20:
            override_flag = "REVIEW"  # flag for human review if >20% off
    else:
        use_amount = calculated_total
        override_flag = False
        override_note = "Calculated baseline \u2014 no sub quote provided"

    return {
        "trade": trade_name,
        "wage_regime": wage_regime,
        "wage_regime_label": CT_WAGE_RATES.get(wage_regime, {}).get("label", wage_regime),
        "line_items": line_items,
        "total_material": round(total_material, 2),
        "total_labor": round(total_labor, 2),
        "calculated_total": round(calculated_total, 2),
        "sub_quote": sub_quote,
        "sub_quote_source": sub_quote_source,
        "use_amount": round(use_amount, 2),
        "sub_quote_override": override_flag,
        "override_note": override_note,
        "confidence": "HIGH" if sub_quote else "MEDIUM",
        "accuracy_note": "Sub quote from named source" if sub_quote else "Calculated baseline \u00b110-15%"
    }


def detect_wage_regime(project_data: dict) -> dict:
    """
    Determines the applicable wage regime based on project characteristics.
    Returns {"regime": str, "confidence": str, "flags": list[str]}

    project_data should contain any of: owner, funding_source, project_type,
    location, stories, notes
    """
    flags = []
    regime = "CT_DOL_RESIDENTIAL"  # safe default
    confidence = "MEDIUM"

    text_to_check = " ".join([
        str(project_data.get("owner", "")),
        str(project_data.get("funding_source", "")),
        str(project_data.get("notes", "")),
        str(project_data.get("project_type", "")),
    ]).lower()

    # Federal funding triggers Davis-Bacon
    federal_keywords = ["hud", "home program", "cdbg", "lihtc", "fha",
                        "usda", "federal", "davis-bacon", "davis bacon"]
    for kw in federal_keywords:
        if kw in text_to_check:
            regime = "DAVIS_BACON_BUILDING_NEW_HAVEN"
            confidence = "HIGH"
            flags.append(f"Federal funding keyword detected: '{kw}' \u2014 Davis-Bacon applies")
            break

    # Non-profit / housing authority suggests prevailing wage likely
    nonprofit_keywords = ["arc", "housing authority", "affordable", "nonprofit",
                          "non-profit", "501c", "community development"]
    for kw in nonprofit_keywords:
        if kw in text_to_check:
            flags.append(f"Non-profit/affordable housing indicator: '{kw}' \u2014 confirm funding source for Davis-Bacon applicability")
            if confidence != "HIGH":
                confidence = "LOW"
            break

    # CT DOT / state roadway work
    dot_keywords = ["ct dot", "dot form 816", "state roadway", "public right of way", "state highway"]
    for kw in dot_keywords:
        if kw in text_to_check:
            flags.append(f"CT DOT / state roadway work detected \u2014 verify prevailing wage for ROW work")
            break

    # Residential building type (confirms CT DOL residential is correct)
    if any(k in text_to_check for k in ["single family", "residential", "apartment", "housing"]):
        if regime == "CT_DOL_RESIDENTIAL":
            confidence = "HIGH"

    flags.append(f"Annual adjustment: CT DOL rates update July 1 each year \u2014 verify before bid")

    return {
        "regime": regime,
        "regime_label": CT_WAGE_RATES.get(regime, {}).get("label", regime),
        "confidence": confidence,
        "flags": flags
    }


def detect_project_type(extraction: dict) -> dict:
    """
    Detects whether this is a standard residential project or a
    specialty/ADA accessible housing project.

    Returns:
    {
        "project_type": "STANDARD_RESIDENTIAL" | "ADA_ACCESSIBLE" | "COMMERCIAL",
        "labor_multiplier": float,  # 1.0 for standard, 1.35 for ADA
        "confidence": "HIGH" | "MEDIUM" | "LOW",
        "trigger_items": list[str],  # what triggered the classification
        "flag_message": str
    }
    """

    ADA_KEYWORDS = [
        "ceiling lift", "grab bar", "roll-in shower", "roll under",
        "arms monitoring", "arms system", "panic button", "nurse call",
        "body dryer", "smart bed", "hospital grade", "medical grade",
        "touchless faucet", "auto dispenser", "accessibility",
        "ada compliance", "ada compliant", "wheelchair", "accessible",
        "supported living", "assisted living", "group home",
        "hydrogiene", "bidette", "shower chair", "transfer bench",
        "hearing loop", "visual alarm", "tactile"
    ]

    SPECIALTY_EQUIPMENT_KEYWORDS = [
        "arms", "ceiling track", "lift system", "smart home integration",
        "remote monitoring", "sensor", "auto", "touchless", "voice control"
    ]

    # Pull all text from extraction to search
    all_text = " ".join([
        str(extraction.get("project_summary", "")),
        str(extraction.get("scope", "")),
        str(extraction.get("equipment_list", "")),
        str(extraction.get("special_requirements", "")),
        str(extraction.get("notes", "")),
        str(extraction.get("owner", "")),
        str(extraction.get("raw_text", "")),
    ]).lower()

    trigger_items = []

    for kw in ADA_KEYWORDS:
        if kw in all_text:
            trigger_items.append(kw)

    specialty_count = sum(1 for kw in SPECIALTY_EQUIPMENT_KEYWORDS if kw in all_text)

    # Count specialty equipment items if equipment list exists
    equipment_list = extraction.get("equipment_list", [])
    equipment_count = len(equipment_list) if isinstance(equipment_list, list) else 0

    # Classification logic
    if len(trigger_items) >= 3 or equipment_count >= 15 or specialty_count >= 3:
        project_type = "ADA_ACCESSIBLE"
        labor_multiplier = 1.35
        confidence = "HIGH" if len(trigger_items) >= 5 else "MEDIUM"
        flag_message = (
            f"ADA/ACCESSIBLE HOUSING DETECTED \u2014 {len(trigger_items)} specialty "
            f"indicators found: {', '.join(trigger_items[:5])}{'...' if len(trigger_items) > 5 else ''}. "
            f"Labor costs adjusted by 1.35x to reflect specialty installation "
            f"requirements, extended coordination time, and ADA-grade workmanship. "
            f"Material costs unchanged. Estimator should verify multiplier."
        )
    elif len(trigger_items) >= 1 or equipment_count >= 8:
        project_type = "ADA_ACCESSIBLE"
        labor_multiplier = 1.20
        confidence = "LOW"
        flag_message = (
            f"POSSIBLE SPECIALTY SCOPE \u2014 {len(trigger_items)} ADA/specialty "
            f"indicators found. Labor adjusted by 1.20x as precaution. "
            f"Estimator must review and confirm project type."
        )
    else:
        project_type = "STANDARD_RESIDENTIAL"
        labor_multiplier = 1.0
        confidence = "HIGH"
        flag_message = "Standard residential scope detected. No ADA labor premium applied."

    return {
        "project_type": project_type,
        "labor_multiplier": labor_multiplier,
        "confidence": confidence,
        "trigger_items": trigger_items,
        "flag_message": flag_message
    }


ADA_LABOR_MULTIPLIER_BY_TRADE = {
    # Trades AFFECTED by ADA — apply premium
    "Plumbing":               1.35,
    "Electrical":             1.30,
    "Smart Home / Security":  1.35,
    "Conveying Equipment":    1.35,
    "Flooring":               1.20,
    "Specialties":            1.25,
    "Countertops & Finishes": 1.20,
    "Cabinets":               1.20,
    "Doors/Hdwr/Finish Carp": 1.20,
    "HVAC":                   1.15,
    # Trades NOT affected by ADA — no premium
    "Building Concrete":      1.0,
    "Structural Steel":       1.0,
    "Rough Carpentry":        1.0,
    "Roofing":                1.0,
    "Siding":                 1.0,
    "Gutters":                1.0,
    "Flashing & Waterproofing": 1.0,
    "Insulation":             1.0,
    "Drywall":                1.0,
    "Painting":               1.0,
    "Windows":                1.0,
    "General Requirements":   1.0,
    "SITE WORK / CIVIL":      1.0,
}


def get_baseline_quantities(trade_name: str, extraction: dict) -> list[dict]:
    """
    Maps a trade name to work items with quantities.
    Quantities are derived from standard residential takeoff logic
    applied to project dimensions extracted from drawings.

    Returns a list of work_item dicts for calculate_trade_estimate().
    Returns empty list if trade cannot be auto-priced.
    """

    # Pull key dimensions from extraction — with fallbacks
    footprint_sf = extraction.get("footprint_sf", 699)
    perimeter_lf = extraction.get("perimeter_lf", 116)
    ext_wall_sf   = extraction.get("ext_wall_sf", 973)
    roof_sq       = extraction.get("roof_squares", 12.0)
    window_count  = extraction.get("window_count", 13)
    ext_door_count = extraction.get("ext_door_count", 5)
    int_door_count = extraction.get("int_door_count", 4)
    plumbing_fixtures = extraction.get("plumbing_fixtures", 6)
    hvac_zones    = extraction.get("hvac_zones", 2)
    cabinet_lf    = extraction.get("cabinet_lf", 16)
    tile_sf       = extraction.get("tile_sf", 180)
    lvt_sf        = extraction.get("lvt_sf", 520)
    electrical_circuits = extraction.get("electrical_circuits", 30)
    foundation_lf = extraction.get("foundation_lf", 116)

    # Interior wall SF estimate (1.8x footprint for residential)
    int_wall_sf = footprint_sf * 1.8
    # Total GWB = both sides of interior walls + one side exterior walls
    gwb_sf = (int_wall_sf * 2) + ext_wall_sf + (footprint_sf * 1.1)  # ceiling

    TRADE_MAP = {
        "Building Concrete": [
            {"work_item": "slab_on_grade_4in",    "quantity": footprint_sf,
             "material_key": "concrete_ready_mix_3000psi",
             "material_quantity": footprint_sf / 81,  # CY: SF / (27 * thickness_ft)
             "description": "4\" slab on grade with WWM and vapor barrier"},
            {"work_item": "footing_continuous",   "quantity": foundation_lf,
             "material_key": "concrete_ready_mix_3000psi",
             "material_quantity": foundation_lf * 0.74,  # CY per LF for 2'x1' footing
             "description": "Continuous footings 2'-0\" x 1'-0\""},
            {"work_item": "foundation_waterproofing", "quantity": foundation_lf * 4,
             "material_key": "foundation_wp_gcp_preprufe160",
             "material_quantity": foundation_lf * 4,
             "description": "GCP Preprufe 160 foundation waterproofing"},
        ],
        "Rough Carpentry": [
            {"work_item": "wall_framing_2x6_ext", "quantity": ext_wall_sf,
             "material_key": "lumber_2x6_kd",
             "material_quantity": ext_wall_sf * 1.1,  # LF lumber per SF wall
             "description": "2x6 exterior wall framing at 16\" OC"},
            {"work_item": "wall_framing_2x4_int", "quantity": int_wall_sf,
             "material_key": "lumber_2x4_kd",
             "material_quantity": int_wall_sf * 1.0,
             "description": "2x4 interior wall framing at 16\" OC"},
            {"work_item": "roof_framing_12_12",   "quantity": footprint_sf,
             "material_key": "lumber_2x10_kd",
             "material_quantity": footprint_sf * 0.8,
             "description": "Roof framing 12/12 pitch \u2014 2x10 rafters"},
            {"work_item": "roof_sheathing",       "quantity": roof_sq * 100,
             "material_key": "plywood_5_8_cdx_roof",
             "material_quantity": roof_sq * 100,
             "description": "5/8\" CDX roof sheathing"},
            {"work_item": "wall_sheathing",       "quantity": ext_wall_sf,
             "material_key": "plywood_1_2_cdx_wall",
             "material_quantity": ext_wall_sf,
             "description": "1/2\" CDX wall sheathing"},
        ],
        "Roofing": [
            {"work_item": "shingle_install",      "quantity": roof_sq,
             "material_key": "shingles_certainteed_xt25",
             "material_quantity": roof_sq,
             "description": "CertainTeed XT-25 Nickel Gray shingles"},
            {"work_item": "ice_water_shield_install", "quantity": roof_sq * 0.25,
             "material_key": "ice_water_shield_grace",
             "material_quantity": roof_sq * 0.25,
             "description": "Grace Ice & Water Shield at rakes/ridges/eaves"},
            {"work_item": "felt_install",         "quantity": roof_sq * 0.75,
             "material_key": "felt_underlayment_30lb",
             "material_quantity": roof_sq * 0.75,
             "description": "30 lb felt underlayment"},
            {"work_item": "drip_edge_install",    "quantity": perimeter_lf,
             "material_key": "drip_edge_aluminum",
             "material_quantity": perimeter_lf,
             "description": "Aluminum drip edge"},
            {"work_item": "gutter_install",       "quantity": perimeter_lf,
             "material_key": "gutter_ogee_5in",
             "material_quantity": perimeter_lf,
             "description": "5\" ogee gutters with leaf screens"},
        ],
        "Siding": [
            {"work_item": "siding_install",       "quantity": ext_wall_sf,
             "material_key": "siding_certainteed_mainstreet_d4",
             "material_quantity": ext_wall_sf * 0.75,
             "description": "CertainTeed Mainstreet Double 4\" Woodgrain"},
            {"work_item": "siding_install",       "quantity": ext_wall_sf * 0.25,
             "material_key": "siding_certainteed_cedar_scallop",
             "material_quantity": ext_wall_sf * 0.25,
             "description": "CertainTeed Cedar Impressions Scallop Colonial White"},
            {"work_item": "housewrap_install",    "quantity": ext_wall_sf,
             "material_key": "housewrap_hydrogap",
             "material_quantity": ext_wall_sf,
             "description": "Benjamin Obdyke HydroGap drainable housewrap"},
            {"work_item": "pvc_trim_install",     "quantity": perimeter_lf * 3,
             "material_key": "pvc_trim_3_4x5_5",
             "material_quantity": perimeter_lf * 3,
             "description": "5-1/2\" PVC trim at windows, doors, corners"},
        ],
        "Insulation": [
            {"work_item": "spray_foam_walls",     "quantity": ext_wall_sf,
             "material_key": "spray_foam_closed_cell_r30",
             "material_quantity": ext_wall_sf,
             "description": "R-30 closed cell spray foam exterior walls"},
            {"work_item": "spray_foam_roof",      "quantity": roof_sq * 100,
             "material_key": "spray_foam_closed_cell_r60",
             "material_quantity": roof_sq * 100,
             "description": "R-60 closed cell spray foam roof assembly"},
            {"work_item": "rigid_insulation_foundation", "quantity": foundation_lf * 4,
             "material_key": "rigid_insulation_r10_2in",
             "material_quantity": foundation_lf * 4,
             "description": "R-10 rigid insulation at foundation"},
        ],
        "Drywall": [
            {"work_item": "gwb_hang",             "quantity": gwb_sf,
             "material_key": "gwb_5_8_type_x",
             "material_quantity": gwb_sf,
             "description": "5/8\" Type X GWB hang"},
            {"work_item": "gwb_tape_finish",      "quantity": gwb_sf,
             "material_key": "joint_compound_tape",
             "material_quantity": gwb_sf,
             "description": "Tape, finish, corner bead"},
        ],
        "Flooring": [
            {"work_item": "lvt_install",          "quantity": lvt_sf,
             "material_key": "lvt_mid_range",
             "material_quantity": lvt_sf,
             "description": "LVT-1 Luxury Vinyl Tile all rooms"},
            {"work_item": "tile_install_floor",   "quantity": tile_sf * 0.6,
             "material_key": "ceramic_tile_floor",
             "material_quantity": tile_sf * 0.6,
             "description": "TL-1 Ceramic tile 4x14 bathroom floor"},
            {"work_item": "tile_install_wall_shower", "quantity": tile_sf * 0.4,
             "material_key": "ceramic_tile_floor",
             "material_quantity": tile_sf * 0.4,
             "description": "TL-1 Ceramic tile bathroom wall/shower"},
        ],
        "Painting": [
            {"work_item": "paint_interior_2coat", "quantity": gwb_sf,
             "material_key": "paint_sw_interior_2coat",
             "material_quantity": gwb_sf,
             "description": "SW Alabaster primer + 2 coats interior"},
            {"work_item": "paint_exterior_trim",  "quantity": perimeter_lf * 3,
             "material_key": "paint_exterior_trim",
             "material_quantity": perimeter_lf * 3,
             "description": "Exterior PVC trim paint"},
        ],
        "Plumbing": [
            {"work_item": "plumbing_rough_per_fixture", "quantity": plumbing_fixtures,
             "material_key": "plumbing_rough_pipe_per_fix",
             "material_quantity": plumbing_fixtures,
             "description": "Supply + drain rough-in per fixture"},
            {"work_item": "plumbing_fixture_set", "quantity": 1,
             "material_key": "toilet_penguin_254",
             "material_quantity": 1,
             "description": "Penguin 254 toilet per spec"},
            {"work_item": "plumbing_fixture_set", "quantity": 1,
             "material_key": "tub_sterling_ensemble_ada",
             "material_quantity": 1,
             "description": "Sterling Ensemble ADA tub per spec"},
            {"work_item": "plumbing_fixture_set", "quantity": 1,
             "material_key": "shower_set_miseno_mia",
             "material_quantity": 1,
             "description": "Miseno Mia shower set per spec"},
        ],
        "Electrical": [
            {"work_item": "panel_install_200amp", "quantity": 1,
             "material_key": "panel_200amp",
             "material_quantity": 1,
             "description": "200A electrical panel"},
            {"work_item": "electrical_rough_per_circuit", "quantity": electrical_circuits,
             "material_key": "wire_per_circuit_rough",
             "material_quantity": electrical_circuits,
             "description": "Branch circuit rough-in wiring"},
            {"work_item": "device_install",       "quantity": 45,
             "material_key": "outlet_device",
             "material_quantity": 30,
             "description": "Outlets, switches, GFI devices"},
            {"work_item": "light_fixture_recessed", "quantity": 12,
             "material_key": "light_recessed_lithonia_wf6",
             "material_quantity": 12,
             "description": "Lithonia WF6 LED recessed lights"},
        ],
        "HVAC": [
            {"work_item": "mini_split_zone_install", "quantity": hvac_zones,
             "material_key": "mitsubishi_mini_split_zone",
             "material_quantity": hvac_zones,
             "description": "Mitsubishi mini-split zones"},
        ],
        "Cabinets": [
            {"work_item": "gwb_hang",             "quantity": 0,
             "material_key": "cabinets_express_rta_per_ln",
             "material_quantity": cabinet_lf,
             "description": "Express Kitchens Barcelona White RTA cabinets"},
            {"work_item": "gwb_hang",             "quantity": 0,
             "material_key": "vanity_36in_ada",
             "material_quantity": 1,
             "description": "36\" ADA vanity per spec"},
        ],
        "Windows": [
            {"work_item": "window_install",       "quantity": window_count,
             "material_key": "window_marvin_ultimate_dh_med",
             "material_quantity": window_count,
             "description": "Marvin Ultimate windows per schedule (13 EA)"},
        ],
        "Doors/Hdwr/Finish Carp": [
            {"work_item": "door_install_exterior", "quantity": ext_door_count,
             "material_key": "door_exterior_insulated_alum",
             "material_quantity": ext_door_count,
             "description": "Insulated aluminum exterior doors"},
            {"work_item": "door_install_exterior", "quantity": int_door_count,
             "material_key": "door_interior_hollow_core",
             "material_quantity": int_door_count,
             "description": "Interior hollow core doors"},
        ],
        "Gutters": [
            {"work_item": "gutter_install", "quantity": 120,
             "material_key": "gutter_ogee_5in", "material_quantity": 120,
             "description": "Ogee gutters 5\" with leaf screens + downspouts"},
        ],
        "SITE WORK / CIVIL": [
            {"work_item": None, "quantity": 0,
             "material_key": None, "material_quantity": 0,
             "description": "PLACEHOLDER \u2014 Civil scope requires sub quotes",
             "override_amount": 148000,
             "note": "\u26a0\ufe0f CIVIL SCOPE PLACEHOLDER $148,000 mid-point estimate. "
                     "Based on: Sitework $45K + Paving $55K + Landscaping $22K + "
                     "Fencing $8K + Site Concrete $18K. "
                     "GET SUB QUOTES BEFORE BID. Range: $95,000\u2013$200,000."}
        ],
        "Conveying Equipment": [
            {"work_item": None, "quantity": 0,
             "material_key": None, "material_quantity": 0,
             "description": "Ceiling lift system BR5 \u2014 structural + track install",
             "override_amount": 15000,
             "note": "\u26a0\ufe0f Ceiling lift placeholder $15,000. "
                     "Midstate supplies equipment, GC installs. "
                     "Range: $8,000\u2013$25,000. Get specialty sub quote."}
        ],
        "Specialties": [
            {"work_item": None, "quantity": 0,
             "material_key": None, "material_quantity": 0,
             "description": "ADA bathroom accessories \u2014 grab bars, mirrors, hardware",
             "override_amount": 4500,
             "note": "Bobrick grab bars, Harney accessories, ADA mirror. "
                     "Per spec A1.02 equipment plan."}
        ],
        "Countertops & Finishes": [
            {"work_item": None, "quantity": 0,
             "material_key": None, "material_quantity": 0,
             "description": "Kitchen + bath countertops CT-1 Dolomite finish",
             "override_amount": 5500,
             "note": "CT-1 countertop Dolomite 12 UL Dick, kitchen + bath. "
                     "Placeholder \u2014 verify LF with sub."}
        ],
        "Smart Home / Security": [
            {"work_item": None, "quantity": 0,
             "material_key": None, "material_quantity": 0,
             "description": "ARMS system rough-in, smart locks, sensors, doorbell",
             "override_amount": 12000,
             "note": "\u26a0\ufe0f ADA Smart Home scope: ARMS panel, motion/door/flood "
                     "sensors, smart locks, video doorbell, camera. "
                     "Rough-in labor + materials. Range: $8,000\u2013$18,000."}
        ],
        "Structural Steel": [
            {"work_item": None, "quantity": 0,
             "material_key": None, "material_quantity": 0,
             "description": "Simpson hardware, LVL beams, anchor bolts, metal connectors",
             "override_amount": 6500,
             "note": "Simpson H2.5A clips, LSTA24 straps, anchor bolts, "
                     "(2) 1-3/4x11-7/8 LVL ridge beam. Per S1.01."}
        ],
        "Flashing & Waterproofing": [
            {"work_item": None, "quantity": 0,
             "material_key": None, "material_quantity": 0,
             "description": "WR Grace Perm-A-Barrier, aluminum flashing, termite shield",
             "override_amount": 4200,
             "note": "WR Grace Perm-A-Barrier membrane flashing, .060 aluminum "
                     "flashing, termite shield. Per A4.01 details."}
        ],
        "General Requirements": [
            {"work_item": None, "quantity": 0,
             "material_key": None, "material_quantity": 0,
             "description": "Survey, temporary facilities, dumpsters, project management",
             "override_amount": 8000,
             "note": "Survey (James W. Seabrook CT PLS #1302), temp power, "
                     "dumpsters, project signage, superintendent time. "
                     "Placeholder \u2014 adjust per GC actual costs."}
        ],
    }

    return TRADE_MAP.get(trade_name, [])


def _detect_site_scope(consolidated: list[dict], trades: list) -> dict:
    """
    Detect whether site drawings / civil scope are present in the extraction.

    Returns:
    {
        "has_site_scope": bool,
        "site_trade": dict or None,  # the consolidated row if found
        "flag_note": str
    }
    """
    SITE_KEYWORDS = [
        "sitework", "earthwork", "grading", "excavat", "demolit",
        "civil", "d-1", "c-1", "landscape", "dot", "paving",
        "ramp", "curb", "utility", "stormwater", "drainage"
    ]

    # Check if any consolidated trade is already site-related
    site_trade = None
    for row in consolidated:
        lower_trade = row["trade"].lower()
        if any(kw in lower_trade for kw in ["sitework", "paving", "landscaping", "fencing", "site concrete"]):
            site_trade = row
            break

    # Also scan raw trades for civil keywords
    raw_text = " ".join([t.trade.lower() + " " + t.scope_description.lower() for t in trades])
    has_keywords = any(kw in raw_text for kw in SITE_KEYWORDS)

    if site_trade:
        return {
            "has_site_scope": True,
            "site_trade": site_trade,
            "flag_note": ""
        }
    elif has_keywords:
        return {
            "has_site_scope": True,
            "site_trade": None,
            "flag_note": "\u26a0\ufe0f SITE SCOPE DETECTED \u2014 quantities not extracted. "
                         "Estimator must price. Typical range: $25,000\u2013$200,000 depending on scope."
        }
    else:
        return {
            "has_site_scope": False,
            "site_trade": None,
            "flag_note": "\u26a0\ufe0f NO SITE DRAWINGS PROVIDED \u2014 if site work exists, this line "
                         "must be priced separately. Do not bid without site scope."
        }


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
    sov_data: Optional[dict] = None,
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

    # ── Project type & site detection ─────────────────────────────────────
    # Build a lightweight extraction dict from the raw trade data for detection
    _raw_text_parts = []
    for t in trades:
        _raw_text_parts.append(t.trade)
        _raw_text_parts.append(t.scope_description)
        if t.vendor_name:
            _raw_text_parts.append(t.vendor_name)
        _raw_text_parts.append(t.evidence)
    for r in requirements:
        _raw_text_parts.append(r.category)
        _raw_text_parts.append(r.requirement)
    _extraction_for_detection = {"raw_text": " ".join(_raw_text_parts)}
    project_type_result = detect_project_type(_extraction_for_detection)
    global_ada_detected = project_type_result.get("project_type") == "ADA_ACCESSIBLE"

    site_result = _detect_site_scope(consolidated, trades)

    wb = openpyxl.Workbook()

    # ── Styles ────────────────────────────────────────────────────────────
    navy_fill = PatternFill(start_color="1A2332", end_color="1A2332", fill_type="solid")
    orange_fill = PatternFill(start_color="E8722A", end_color="E8722A", fill_type="solid")
    light_gray_fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
    summary_fill = PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid")
    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    amber_fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    site_fill = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")
    blue_rate_font = Font(name="Calibri", bold=True, size=11, color="0000FF")
    light_blue_fill = PatternFill(start_color="DDEEFF", end_color="DDEEFF", fill_type="solid")
    dark_blue_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    dark_green_fill = PatternFill(start_color="375623", end_color="375623", fill_type="solid")
    project_total_fill = PatternFill(start_color="1F2D3D", end_color="1F2D3D", fill_type="solid")

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
    NUM_COLS = 7  # A through G

    # Column layout: DIV | TRADE | SCOPE | LOW BIDDER | BUDGET / SOV | VARIANCE | NOTES
    col_headers = [
        ("A", "DIV", 7),
        ("B", "TRADE", 34),
        ("C", "SCOPE", 62),
        ("D", "LOW BIDDER", 22),
        ("E", "BUDGET / SOV", 18),
        ("F", "VARIANCE", 14),
        ("G", "NOTES", 36),
    ]

    # Match SOV values from reference buyout if provided
    sov_matched = {}
    sov_summary = {"ohp": None, "bond": None, "permit": None}
    if sov_data:
        sov_matched = _match_sov_to_trades(
            consolidated, sov_data.get("trade_sov", {})
        )
        sov_summary["ohp"] = sov_data.get("ohp")
        sov_summary["bond"] = sov_data.get("bond")
        sov_summary["permit"] = sov_data.get("permit")

    # Filter to trades with pricing data OR pricing engine coverage
    buyout_rows = [
        row for row in consolidated
        if row.get("budget") is not None
        or row["trade"] in sov_matched
        or get_baseline_quantities(row["trade"], _extraction_for_detection)
    ]
    # Safety fallback: if nothing has pricing, show everything
    if not buyout_rows:
        buyout_rows = consolidated

    # Row 1: title bar
    ws.merge_cells("A1:G1")
    title_cell = ws["A1"]
    title_cell.value = "BuildBrain Buyout Summary"
    title_cell.font = title_font
    title_cell.fill = navy_fill
    title_cell.alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[1].height = 40

    # Row 2: PROJECT TYPE banner (when ADA/specialty detected)
    header_row = 2
    if project_type_result["project_type"] != "STANDARD_RESIDENTIAL":
        banner_text = (
            f"\u26a0\ufe0f PROJECT TYPE: {project_type_result['project_type'].replace('_', ' ')}  |  "
            f"Labor Multiplier: per-trade (see Notes)  |  "
            f"{', '.join(project_type_result['trigger_items'][:3])}"
        )
        ws.merge_cells("A2:G2")
        banner_cell = ws["A2"]
        banner_cell.value = banner_text
        banner_cell.font = Font(name="Calibri", bold=True, size=11)
        banner_cell.fill = amber_fill
        banner_cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
        ws.row_dimensions[2].height = 32
        header_row = 3

    # Column headers
    for col_letter, label, width in col_headers:
        cell = ws[f"{col_letter}{header_row}"]
        cell.value = label
        cell.font = header_font
        cell.fill = navy_fill
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = thin_border
        ws.column_dimensions[col_letter].width = width
    ws.row_dimensions[header_row].height = 30

    # Data rows start after header
    data_start = header_row + 1

    # ── Site Work / Civil row (always first trade row) ────────────────
    site_row_idx = data_start
    site_in_buyout = site_result["has_site_scope"] and site_result["site_trade"] is not None
    # Check if Sitework is already in buyout_rows
    site_already_in_buyout = any(
        r["trade"] in ("Sitework", "Paving & Striping", "Site Concrete")
        for r in buyout_rows
    )
    if not site_already_in_buyout:
        # Add a SITE WORK / CIVIL placeholder row with pricing from TRADE_MAP
        site_items = get_baseline_quantities("SITE WORK / CIVIL", _extraction_for_detection)
        site_override = next((item.get("override_amount") for item in site_items
                             if item.get("override_amount")), 0)
        site_override_note = next((item.get("note") for item in site_items
                                  if item.get("note") and item.get("override_amount")),
                                 site_result["flag_note"])
        ws.cell(row=site_row_idx, column=1, value="02000").font = bold_font
        ws.cell(row=site_row_idx, column=2, value="SITE WORK / CIVIL").font = bold_font
        ws.cell(row=site_row_idx, column=3, value="See Scope Details tab").alignment = wrap_top
        ws.cell(row=site_row_idx, column=5, value=site_override).number_format = money_fmt
        ws.cell(row=site_row_idx, column=7, value=site_override_note).alignment = wrap_top
        ws.cell(row=site_row_idx, column=7).font = Font(name="Calibri", size=9, italic=True)
        for col in range(1, NUM_COLS + 1):
            ws.cell(row=site_row_idx, column=col).fill = site_fill
            ws.cell(row=site_row_idx, column=col).border = thin_border
        ws.row_dimensions[site_row_idx].height = 36
        data_start = site_row_idx + 1
    for i, row_data in enumerate(buyout_rows):
        labor_multiplier = 1.0  # default — overridden below if ADA detected
        r = data_start + i
        ws.cell(row=r, column=1, value=int(row_data["csi_division"])).font = bold_font
        ws.cell(row=r, column=2, value=row_data["trade"]).font = bold_font

        # Brief scope: first 3 items, truncated
        scope_lines = row_data["scope"].split("\n")
        brief = "; ".join(line.split(". ", 1)[-1] for line in scope_lines[:3])
        if len(scope_lines) > 3:
            brief += f" (+{len(scope_lines) - 3} more)"
        ws.cell(row=r, column=3, value=brief).alignment = wrap_top

        # LOW BIDDER (D): vendor name of lowest bid
        low_bidder_cell = ws.cell(row=r, column=4)
        bids = row_data.get("bids", [])
        if bids:
            low_bidder_cell.value = bids[0][0]  # vendor name of lowest bid
        low_bidder_cell.alignment = top_align

        # BUDGET / SOV (E): from reference buyout, extracted budget, or pricing engine
        trade_name = row_data["trade"]
        budget_val = None
        note_val = None

        # Tier 1: SOV match from uploaded reference
        if trade_name in sov_matched:
            budget_val = sov_matched[trade_name]

        # Tier 2: Claude extracted dollar amount from PDFs
        if budget_val is None and row_data.get("budget") is not None:
            budget_val = row_data["budget"]

        # Tier 3: Calculate baseline using wage rates + material DB
        if budget_val is None or budget_val == 0:
            work_items = get_baseline_quantities(trade_name, _extraction_for_detection)
            if work_items:
                # Check for override_amount (placeholder trades)
                override = next((item.get("override_amount") for item in work_items
                                if item.get("override_amount")), None)
                override_note = next((item.get("note") for item in work_items
                                     if item.get("note") and item.get("override_amount")), None)

                if override is not None:
                    # Placeholder trade — write directly, skip calculate_trade_estimate
                    budget_val = override
                    note_val = override_note
                else:
                    # Normal trade — run the pricing engine
                    wage_regime = "CT_DOL_RESIDENTIAL"
                    if isinstance(project_type_result, dict) and "regime" in project_type_result:
                        wage_regime = project_type_result["regime"]

                    trade_estimate = calculate_trade_estimate(
                        trade_name=trade_name,
                        work_items=work_items,
                        wage_regime=wage_regime
                    )

                    # Apply per-trade ADA labor multiplier AFTER base calculation
                    if global_ada_detected:
                        labor_multiplier = ADA_LABOR_MULTIPLIER_BY_TRADE.get(trade_name, 1.0)
                    else:
                        labor_multiplier = 1.0

                    if labor_multiplier != 1.0:
                        adj_labor = trade_estimate["total_labor"] * labor_multiplier
                        budget_val = trade_estimate["total_material"] + adj_labor
                        note_val = (
                            f"Calculated baseline | Material: ${trade_estimate['total_material']:,.0f} "
                            f"+ Labor: ${trade_estimate['total_labor']:,.0f} \u00d7 {labor_multiplier}x ADA "
                            f"= ${budget_val:,.0f} | \u00b115% accuracy | Sub quote overrides this"
                        )
                    else:
                        budget_val = trade_estimate["use_amount"]
                        note_val = (
                            f"Calculated baseline | Material: ${trade_estimate['total_material']:,.0f} "
                            f"+ Labor: ${trade_estimate['total_labor']:,.0f} \u00d7 1.0x standard "
                            f"= ${budget_val:,.0f} | \u00b115% accuracy | Sub quote overrides this"
                        )
            else:
                budget_val = 0
                note_val = "\u26a0\ufe0f No pricing found \u2014 estimator must price this trade"
        else:
            note_val = "Extracted from documents or SOV match"

        # Write to column E (always, never blank)
        ws.cell(row=r, column=5, value=round(budget_val) if budget_val else 0).number_format = money_fmt

        # VARIANCE (F): blank for user
        ws.cell(row=r, column=6, value="").font = normal_font
        # NOTES (G): pricing source info
        existing_g = ws.cell(row=r, column=7).value
        if not existing_g:
            ws.cell(row=r, column=7, value=note_val).font = Font(
                name="Calibri", size=9, italic=True)

        # Alternate row shading
        if i % 2 == 1:
            for col in range(1, NUM_COLS + 1):
                ws.cell(row=r, column=col).fill = light_gray_fill

        # All cells get border
        for col in range(1, NUM_COLS + 1):
            ws.cell(row=r, column=col).border = thin_border

        ws.row_dimensions[r].height = max(30, min(len(scope_lines) * 18, 90))

    data_end = data_start + len(buyout_rows) - 1

    # ── Python-calculated summary values ─────────────────────────────────
    # Collect all building trade E values (excludes site row)
    building_vals = []
    for r in range(data_start, data_end + 1):
        v = ws.cell(row=r, column=5).value
        if isinstance(v, (int, float)):
            building_vals.append(v)
    building_subtotal = sum(building_vals)

    site_subtotal = ws.cell(row=site_row_idx, column=5).value or 0
    if not isinstance(site_subtotal, (int, float)):
        site_subtotal = 0

    gc_oh_rate       = 0.10
    gc_profit_rate   = 0.12
    contingency_rate = 0.10
    permits_rate     = 0.025
    site_fee_rate    = 0.05

    gc_oh          = round(building_subtotal * gc_oh_rate)
    gc_profit      = round(building_subtotal * gc_profit_rate)
    contingency    = round(building_subtotal * contingency_rate)
    permits        = round(building_subtotal * permits_rate)
    building_total = building_subtotal + gc_oh + gc_profit + contingency + permits

    site_fee       = round(site_subtotal * site_fee_rate)
    site_total     = site_subtotal + site_fee
    project_total  = building_total + site_total

    currency_fmt = '$#,##0'

    # ── Summary rows below data ──────────────────────────────────────────
    gap = data_end + 2

    # ── SECTION A: BUILDING TRADES ────────────────────────────────────
    bldg_sub_row = gap
    ws.cell(row=bldg_sub_row, column=2, value="BUILDING SUBTOTAL").font = bold_font
    ws.cell(row=bldg_sub_row, column=5, value=building_subtotal).number_format = currency_fmt
    ws.cell(row=bldg_sub_row, column=5).font = bold_font
    ws.cell(row=bldg_sub_row, column=7,
            value=f"Sum of {len(building_vals)} building trades").font = Font(
        name="Calibri", size=9, italic=True, color="666666")
    for col in range(1, NUM_COLS + 1):
        ws.cell(row=bldg_sub_row, column=col).fill = light_blue_fill
        ws.cell(row=bldg_sub_row, column=col).border = thin_border
    ws.row_dimensions[bldg_sub_row].height = 28

    # GC General Conditions (10%)
    gc_cond_row = bldg_sub_row + 1
    ws.cell(row=gc_cond_row, column=2, value="GC General Conditions").font = bold_font
    ws.cell(row=gc_cond_row, column=5, value=gc_oh).number_format = currency_fmt
    ws.cell(row=gc_cond_row, column=7,
            value=f"${building_subtotal:,.0f} \u00d7 {gc_oh_rate:.0%}").font = Font(
        name="Calibri", size=9, italic=True, color="666666")
    for col in range(1, NUM_COLS + 1):
        ws.cell(row=gc_cond_row, column=col).border = thin_border
    ws.row_dimensions[gc_cond_row].height = 28

    # GC Profit (12%, green background)
    gc_profit_row = gc_cond_row + 1
    ws.cell(row=gc_profit_row, column=2, value="GC Profit").font = bold_font
    ws.cell(row=gc_profit_row, column=5, value=gc_profit).number_format = currency_fmt
    ws.cell(row=gc_profit_row, column=7,
            value=f"${building_subtotal:,.0f} \u00d7 {gc_profit_rate:.0%}").font = Font(
        name="Calibri", size=9, italic=True, color="666666")
    for col in range(1, NUM_COLS + 1):
        ws.cell(row=gc_profit_row, column=col).fill = green_fill
        ws.cell(row=gc_profit_row, column=col).border = thin_border
    ws.row_dimensions[gc_profit_row].height = 28

    # Contingency (10%, amber background)
    contingency_row = gc_profit_row + 1
    ws.cell(row=contingency_row, column=2, value="Contingency").font = bold_font
    ws.cell(row=contingency_row, column=5, value=contingency).number_format = currency_fmt
    ws.cell(row=contingency_row, column=7,
            value=f"${building_subtotal:,.0f} \u00d7 {contingency_rate:.0%}").font = Font(
        name="Calibri", size=9, italic=True, color="666666")
    for col in range(1, NUM_COLS + 1):
        ws.cell(row=contingency_row, column=col).fill = amber_fill
        ws.cell(row=contingency_row, column=col).border = thin_border
    ws.row_dimensions[contingency_row].height = 28

    # Permits + Bond (2.5%)
    permits_bond_row = contingency_row + 1
    ws.cell(row=permits_bond_row, column=2, value="Permits + Bond").font = bold_font
    ws.cell(row=permits_bond_row, column=5, value=permits).number_format = currency_fmt
    ws.cell(row=permits_bond_row, column=7,
            value=f"${building_subtotal:,.0f} \u00d7 {permits_rate:.1%}").font = Font(
        name="Calibri", size=9, italic=True, color="666666")
    for col in range(1, NUM_COLS + 1):
        ws.cell(row=permits_bond_row, column=col).border = thin_border
    ws.row_dimensions[permits_bond_row].height = 28

    # BUILDING TOTAL row (dark blue background, white bold)
    bldg_total_row = permits_bond_row + 1
    ws.cell(row=bldg_total_row, column=2, value="BUILDING TOTAL").font = Font(
        name="Calibri", bold=True, size=12, color="FFFFFF")
    ws.cell(row=bldg_total_row, column=5, value=building_total).number_format = currency_fmt
    ws.cell(row=bldg_total_row, column=5).font = Font(
        name="Calibri", bold=True, size=12, color="FFFFFF")
    ws.cell(row=bldg_total_row, column=7,
            value=f"${building_subtotal:,.0f} + markups").font = Font(
        name="Calibri", size=9, italic=True, color="FFFFFF")
    for col in range(1, NUM_COLS + 1):
        ws.cell(row=bldg_total_row, column=col).fill = dark_blue_fill
        ws.cell(row=bldg_total_row, column=col).font = Font(
            name="Calibri", bold=True, size=12, color="FFFFFF")
        ws.cell(row=bldg_total_row, column=col).border = thin_border
    ws.row_dimensions[bldg_total_row].height = 32

    # Spacer between sections
    spacer1_row = bldg_total_row + 1
    ws.row_dimensions[spacer1_row].height = 10

    # ── SECTION B: SITE WORK ─────────────────────────────────────────
    site_sub_row = spacer1_row + 1
    ws.cell(row=site_sub_row, column=2, value="SITE WORK SUBTOTAL").font = bold_font
    ws.cell(row=site_sub_row, column=5, value=site_subtotal).number_format = currency_fmt
    ws.cell(row=site_sub_row, column=5).font = bold_font
    for col in range(1, NUM_COLS + 1):
        ws.cell(row=site_sub_row, column=col).fill = site_fill
        ws.cell(row=site_sub_row, column=col).border = thin_border
    ws.row_dimensions[site_sub_row].height = 28

    # GC Management Fee (5%)
    site_fee_row = site_sub_row + 1
    ws.cell(row=site_fee_row, column=2, value="GC Management Fee").font = bold_font
    ws.cell(row=site_fee_row, column=5, value=site_fee).number_format = currency_fmt
    ws.cell(row=site_fee_row, column=7,
            value=f"${site_subtotal:,.0f} \u00d7 {site_fee_rate:.0%} GC mgmt fee").font = Font(
        name="Calibri", size=9, italic=True, color="666666")
    for col in range(1, NUM_COLS + 1):
        ws.cell(row=site_fee_row, column=col).border = thin_border
    ws.row_dimensions[site_fee_row].height = 28

    # SITE TOTAL row (dark green background, white bold)
    site_total_row = site_fee_row + 1
    ws.cell(row=site_total_row, column=2, value="SITE TOTAL").font = Font(
        name="Calibri", bold=True, size=12, color="FFFFFF")
    ws.cell(row=site_total_row, column=5, value=site_total).number_format = currency_fmt
    ws.cell(row=site_total_row, column=5).font = Font(
        name="Calibri", bold=True, size=12, color="FFFFFF")
    for col in range(1, NUM_COLS + 1):
        ws.cell(row=site_total_row, column=col).fill = dark_green_fill
        ws.cell(row=site_total_row, column=col).font = Font(
            name="Calibri", bold=True, size=12, color="FFFFFF")
        ws.cell(row=site_total_row, column=col).border = thin_border
    ws.row_dimensions[site_total_row].height = 32

    # Explanatory note below SITE TOTAL
    note_row = site_total_row + 1
    ws.merge_cells(f"C{note_row}:G{note_row}")
    note_cell = ws.cell(row=note_row, column=3)
    note_cell.value = (
        "Civil sub quotes are lump sum and include sub\u2019s overhead & profit. "
        "GC management fee only (5%) applied to site scope. "
        "Building trades include full GC overhead, profit, and contingency."
    )
    note_cell.font = Font(name="Calibri", size=9, italic=True, color="666666")
    note_cell.alignment = wrap_top
    ws.row_dimensions[note_row].height = 36

    # Spacer before project total
    spacer2_row = note_row + 1
    ws.row_dimensions[spacer2_row].height = 10

    # ── SECTION C: PROJECT TOTAL ─────────────────────────────────────
    project_total_row = spacer2_row + 1
    ws.cell(row=project_total_row, column=2, value="PROJECT TOTAL").font = Font(
        name="Calibri", bold=True, size=14, color="FFFFFF")
    ws.cell(row=project_total_row, column=5, value=project_total).number_format = currency_fmt
    ws.cell(row=project_total_row, column=5).font = Font(
        name="Calibri", bold=True, size=14, color="FFFFFF")
    ws.cell(row=project_total_row, column=7,
            value=f"Building ${building_total:,.0f} + Site ${site_total:,.0f}").font = Font(
        name="Calibri", size=9, italic=True, color="FFFFFF")
    for col in range(1, NUM_COLS + 1):
        ws.cell(row=project_total_row, column=col).fill = project_total_fill
        ws.cell(row=project_total_row, column=col).font = Font(
            name="Calibri", bold=True, size=14, color="FFFFFF")
        ws.cell(row=project_total_row, column=col).border = thin_border
    ws.row_dimensions[project_total_row].height = 36

    # Freeze panes (adjust for banner row)
    ws.freeze_panes = f"C{header_row + 1}"
    ws.sheet_properties.tabColor = "1A2332"

    # ── TAB 2: Scope Details ──────────────────────────────────────────────
    ws2 = wb.create_sheet("Scope Details")
    ws2.sheet_properties.tabColor = "E8722A"

    scope_headers = ["DIV", "TRADE", "SCOPE", "SOURCE PAGES"]
    for ci, h in enumerate(scope_headers, 1):
        cell = ws2.cell(row=1, column=ci, value=h)
        cell.font = header_font
        cell.fill = navy_fill
        cell.border = thin_border

    ws2.column_dimensions["A"].width = 7
    ws2.column_dimensions["B"].width = 34
    ws2.column_dimensions["C"].width = 110
    ws2.column_dimensions["D"].width = 55
    ws2.row_dimensions[1].height = 28

    for i, row_data in enumerate(consolidated):
        r = i + 2
        ws2.cell(row=r, column=1, value=int(row_data["csi_division"])).alignment = top_align
        ws2.cell(row=r, column=2, value=row_data["trade"]).alignment = top_align
        ws2.cell(row=r, column=2).font = bold_font
        scope_cell = ws2.cell(row=r, column=3, value=row_data["scope"])
        scope_cell.alignment = wrap_top
        ws2.cell(row=r, column=4, value=row_data["source_pages"]).alignment = wrap_top
        line_count = row_data["scope"].count("\n") + 1
        ws2.row_dimensions[r].height = max(30, line_count * 18)
        for ci in range(1, 5):
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
