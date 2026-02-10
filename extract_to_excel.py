"""
BuildBrain Doc->Spreadsheet PoC  (CLI)
=======================================
Extracts submission requirements and trades/scope from construction PDF
drawings using Claude Vision API, then outputs a structured Excel workbook.

Usage:
    python extract_to_excel.py
"""

import os
import sys
from pathlib import Path

# Force UTF-8 output on Windows to avoid cp1252 encoding errors
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

from engine import INPUT_RATE, OUTPUT_RATE, build_excel_bytes, process_pdfs

# ─── Load environment ────────────────────────────────────────────────────────
load_dotenv()

API_KEY = os.environ.get("ANTHROPIC_API_KEY")
if not API_KEY:
    print("ERROR: ANTHROPIC_API_KEY not set. Export it or add to .env file.")
    sys.exit(1)

# ─── PDF paths (edit these if your PDFs are elsewhere) ───────────────────────
DESKTOP = Path(os.environ.get("BUILDBRAIN_PDF_DIR", ""))
if not DESKTOP.exists() or str(DESKTOP) == ".":
    candidates = [
        Path.home() / "OneDrive" / "Desktop",
        Path.home() / "Desktop",
        Path.cwd(),
    ]
    for c in candidates:
        if (c / "Site Drawings.pdf").exists():
            DESKTOP = c
            break
    else:
        DESKTOP = Path.cwd()

PDF_FILES = [
    DESKTOP / "Site Drawings.pdf",
    DESKTOP / "Midstate House Drawings.pdf",
]

OUTPUT_FILE = Path(__file__).parent / "buildbrain_output.xlsx"


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    print("=" * 65)
    print("  BuildBrain Doc -> Spreadsheet PoC")
    print("=" * 65)

    # Console progress callback
    def on_progress(msg: str):
        print(f"  {msg}")

    requirements, trades, stats = process_pdfs(PDF_FILES, API_KEY, progress_callback=on_progress)

    # Build Excel and write to disk
    print(f"\n{'─' * 65}")
    print("Building Excel workbook...")
    excel_bytes = build_excel_bytes(requirements, trades)
    OUTPUT_FILE.write_bytes(excel_bytes)

    print(f"  Output: {OUTPUT_FILE}")
    print(f"  Tabs: Submission_Requirements ({stats['num_requirements']} rows), "
          f"Trades_and_Scope ({stats['num_trades']} rows), "
          f"Source_Trace ({stats['num_requirements'] + stats['num_trades']} rows)")

    # ─── Token / Cost Summary ─────────────────────────────────────────────
    print(f"\n{'=' * 65}")
    print("  TOKEN & COST SUMMARY")
    print(f"{'=' * 65}")
    print(f"  Total pages processed : {stats['total_pages']}")
    print(f"  Total API calls       : {stats['api_calls']}")
    print(f"  Total input tokens    : {stats['input_tokens']:,}")
    print(f"  Total output tokens   : {stats['output_tokens']:,}")
    print(f"  Input cost            : ${stats['input_cost']:.4f}")
    print(f"  Output cost           : ${stats['output_cost']:.4f}")
    print(f"  TOTAL COST            : ${stats['total_cost']:.4f}")

    pdf_stats = stats.get("pdf_stats", {})
    if pdf_stats:
        print(f"\n  {'─' * 55}")
        print("  Cost breakdown by PDF:")
        for pdf_name, ps in pdf_stats.items():
            p_input_cost = (ps["input_tokens"] / 1_000_000) * INPUT_RATE
            p_output_cost = (ps["output_tokens"] / 1_000_000) * OUTPUT_RATE
            p_total = p_input_cost + p_output_cost
            print(f"\n    {pdf_name}")
            print(f"      Pages     : {ps['pages']}")
            print(f"      API calls : {ps['api_calls']}")
            print(f"      Input tok : {ps['input_tokens']:,}")
            print(f"      Output tok: {ps['output_tokens']:,}")
            print(f"      Cost      : ${p_total:.4f}")

    if stats["failed_pages"]:
        print(f"\n  {'─' * 55}")
        print(f"  Failed pages ({len(stats['failed_pages'])}):")
        for fp in stats["failed_pages"]:
            print(f"    - {fp}")

    print(f"\n{'=' * 65}")
    print("  DONE")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
