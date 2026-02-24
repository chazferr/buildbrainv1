"""
BuildBrain Web App
===================
Flask web server providing a drag-and-drop PDF upload UI for construction
document extraction.

Usage:
    python app.py
    # Then open http://localhost:5000
"""

import json
import os
import shutil
import threading
import time
import uuid
from pathlib import Path
from queue import Empty, Queue

from dotenv import load_dotenv
from flask import Flask, Response, render_template, request, jsonify, send_file

from engine import (build_excel_bytes, process_files, parse_sov_from_buyout,
                    extract_project_quantities, validate_project_quantities,
                    classify_project_complexity, SUPPORTED_EXTENSIONS)

# ─── Load environment ────────────────────────────────────────────────────────
load_dotenv()

API_KEY = os.environ.get("ANTHROPIC_API_KEY")
if not API_KEY:
    raise RuntimeError("ANTHROPIC_API_KEY not set. Export it or add to .env file.")

# ─── Flask app ────────────────────────────────────────────────────────────────

app = Flask(__name__)

BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

# In-memory job store: job_id -> {status, queue, excel_bytes, stats, error}
jobs: dict[str, dict] = {}

# ─── IP Whitelist ─────────────────────────────────────────────────────────────

ALLOWED_IPS = {
    "127.0.0.1",        # localhost
    "::1",              # localhost IPv6
    "10.0.0.18",        # your local network IP
    "71.235.242.49",    # external allowed IP
}


@app.before_request
def check_ip_whitelist():
    client_ip = request.remote_addr
    if client_ip not in ALLOWED_IPS:
        return jsonify({"error": "Access denied"}), 403


# ─── Routes ──────────────────────────────────────────────────────────────────


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/upload", methods=["POST"])
def upload():
    """Accept file uploads (PDFs, DOCX, XLSX, EML, images, etc.) and return a job_id."""
    files = request.files.getlist("pdfs")
    if not files or all(f.filename == "" for f in files):
        return jsonify({"error": "No files uploaded"}), 400

    job_id = uuid.uuid4().hex[:12]
    job_dir = UPLOAD_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    saved_paths = []
    for f in files:
        if f.filename:
            ext = Path(f.filename).suffix.lower()
            if ext in SUPPORTED_EXTENSIONS:
                safe_name = Path(f.filename).name  # strip directory components
                dest = job_dir / safe_name
                f.save(str(dest))
                saved_paths.append(dest)

    if not saved_paths:
        shutil.rmtree(job_dir, ignore_errors=True)
        return jsonify({"error": "No supported files found. Accepted: PDF, DOCX, XLSX, CSV, EML, JPG, PNG, TXT"}), 400

    # Handle optional reference buyout spreadsheet for SOV mapping
    buyout_path = None
    buyout_file = request.files.get("buyout")
    if buyout_file and buyout_file.filename:
        buyout_ext = Path(buyout_file.filename).suffix.lower()
        if buyout_ext in (".xlsx", ".xls"):
            safe_buyout = "buyout_ref" + buyout_ext
            buyout_dest = job_dir / safe_buyout
            buyout_file.save(str(buyout_dest))
            buyout_path = buyout_dest

    # Read confirmed pre-check values (if user went through pre-check flow)
    confirmed_values = {}
    for key in ("confirmed_project_name", "confirmed_project_address",
                "confirmed_project_type", "confirmed_construction_type",
                "confirmed_unit_count", "confirmed_floor_count",
                "confirmed_total_building_sf"):
        val = request.form.get(key)
        if val:
            confirmed_values[key] = val

    # Initialize job
    progress_queue: Queue = Queue()
    jobs[job_id] = {
        "status": "processing",
        "queue": progress_queue,
        "excel_bytes": None,
        "stats": None,
        "error": None,
        "file_paths": saved_paths,
        "buyout_path": buyout_path,
        "job_dir": job_dir,
        "confirmed_values": confirmed_values,
    }

    # Start extraction in background thread
    thread = threading.Thread(
        target=_run_extraction,
        args=(job_id,),
        daemon=True,
    )
    thread.start()

    return jsonify({"job_id": job_id, "files": [p.name for p in saved_paths]})


@app.route("/api/stream/<job_id>")
def stream(job_id):
    """SSE endpoint streaming progress events for a job."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    def event_stream():
        q = job["queue"]
        while True:
            try:
                msg = q.get(timeout=10)
            except Empty:
                # Send keepalive heartbeat every 10s to prevent proxy/browser timeouts
                yield ": heartbeat\n\n"
                continue

            if msg is None:
                # Sentinel: job finished
                if job["error"]:
                    yield f"event: error\ndata: {json.dumps({'error': job['error']})}\n\n"
                else:
                    yield f"event: done\ndata: {json.dumps(job['stats'])}\n\n"
                break
            else:
                yield f"event: progress\ndata: {json.dumps({'message': msg})}\n\n"

    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/download/<job_id>")
def download(job_id):
    """Download the generated Excel file for a completed job."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    if job["status"] != "done":
        return jsonify({"error": "Job not finished yet"}), 409
    if job["excel_bytes"] is None:
        return jsonify({"error": "No Excel file available"}), 500

    from io import BytesIO
    buf = BytesIO(job["excel_bytes"])
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="buildbrain_output.xlsx",
    )


@app.route("/api/pre-check", methods=["POST"])
def pre_check():
    """Read first 3 pages of each PDF and return project dimensions immediately."""
    import anthropic
    import base64
    import fitz  # PyMuPDF

    files = request.files.getlist("pdfs")
    if not files:
        return jsonify({"error": "No files uploaded"}), 400

    page_texts = []
    for f in files:
        try:
            f.seek(0)
            doc = fitz.open(stream=f.read(), filetype="pdf")
            pages_to_read = min(3, len(doc))
            for i in range(pages_to_read):
                page = doc[i]
                text = page.get_text() or ""
                if text.strip():
                    page_texts.append(
                        f"[File: {f.filename}, Page {i + 1}]\n{text[:2000]}"
                    )
            doc.close()
        except Exception as e:
            page_texts.append(f"[File: {f.filename} — could not read: {e}]")

    if not page_texts:
        return jsonify({"error": "No readable content in first pages"}), 400

    combined = "\n\n".join(page_texts[:12])

    prompt = (
        "You are analyzing construction document cover sheets.\n"
        "Extract project information and return ONLY valid JSON.\n"
        "No explanation, no markdown, just the JSON object.\n\n"
        "{\n"
        '  "project_name": "name of the project",\n'
        '  "project_address": "full address",\n'
        '  "owner": "owner name",\n'
        '  "architect": "architect name",\n'
        '  "project_type": "single_family" | "multi_family" | '
        '"commercial" | "mixed_use" | "unknown",\n'
        '  "construction_type": "new_construction" | "renovation" | '
        '"gut_rehabilitation" | "addition" | "unknown",\n'
        '  "unit_count": "<integer or null>",\n'
        '  "floor_count": "<integer or null>",\n'
        '  "total_building_sf": "<integer or null>",\n'
        '  "bid_date": "date string or null",\n'
        '  "confidence": "high" | "medium" | "low",\n'
        '  "notes": "brief note on what was found or missing"\n'
        "}\n\n"
        "If a field cannot be determined, use null.\n"
        "Do not guess — only extract what is explicitly stated.\n\n"
        f"DOCUMENT PAGES:\n{combined}"
    )

    try:
        client = anthropic.Anthropic(api_key=API_KEY)
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        text = text.replace("```json", "").replace("```", "").strip()
        result = json.loads(text)

        # Run construction type classifier on result
        result = classify_project_complexity(result, page_texts)

        return jsonify(result)

    except json.JSONDecodeError:
        return jsonify({
            "error": "Could not parse project info",
            "raw": text[:500],
            "confidence": "low",
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── Background worker ──────────────────────────────────────────────────────


def _run_extraction(job_id: str):
    """Run the PDF extraction in a background thread."""
    job = jobs[job_id]
    q = job["queue"]

    def on_progress(msg: str):
        q.put(msg)

    try:
        requirements, trades, stats = process_files(
            job["file_paths"],
            API_KEY,
            progress_callback=on_progress,
        )

        # Parse SOV from reference buyout spreadsheet if provided
        sov_data = None
        buyout_path = job.get("buyout_path")
        if buyout_path and Path(buyout_path).exists():
            on_progress("Parsing SOV from reference buyout spreadsheet...")
            try:
                sov_data = parse_sov_from_buyout(Path(buyout_path))
                matched_count = len(sov_data.get("trade_sov", {}))
                on_progress(f"Found {matched_count} trade SOV values from buyout")
            except Exception as e:
                on_progress(f"[WARNING] Could not parse buyout: {e}")

        # Extract project quantities from all trade data
        on_progress("Extracting project quantities from documents...")
        project_quantities = extract_project_quantities(trades, on_progress, API_KEY)

        # Merge confirmed pre-check values (user overrides from UI)
        confirmed = job.get("confirmed_values", {})
        if confirmed:
            on_progress("Applying user-confirmed project values...")
            _CONFIRM_MAP = {
                "confirmed_project_type": "project_type",
                "confirmed_construction_type": "construction_type",
                "confirmed_unit_count": "unit_count",
                "confirmed_floor_count": "floor_count",
                "confirmed_total_building_sf": "total_building_sf",
                "confirmed_project_name": "project_name",
                "confirmed_project_address": "project_address",
            }
            _INT_FIELDS = {"unit_count", "floor_count", "total_building_sf"}
            for ckey, pkey in _CONFIRM_MAP.items():
                cval = confirmed.get(ckey)
                if cval and cval != "unknown":
                    if pkey in _INT_FIELDS:
                        try:
                            project_quantities[pkey] = int(cval)
                        except (ValueError, TypeError):
                            pass
                    else:
                        project_quantities[pkey] = cval

            # Recalculate complexity multiplier based on confirmed construction_type
            ctype = project_quantities.get("construction_type", "new_construction")
            _MULT_MAP = {
                "new_construction": 1.0,
                "renovation": 1.35,
                "gut_rehabilitation": 1.55,
                "addition": 1.20,
            }
            project_quantities["complexity_multiplier"] = _MULT_MAP.get(ctype, 1.0)
            if project_quantities["complexity_multiplier"] != 1.0:
                project_quantities["complexity_warning"] = (
                    f"{ctype.replace('_', ' ').title()} project — "
                    f"{project_quantities['complexity_multiplier']:.2f}x multiplier applied"
                )

        project_quantities = validate_project_quantities(project_quantities, on_progress)
        on_progress(f"Project type: {project_quantities.get('project_type', 'unknown')}")
        on_progress(f"Units: {project_quantities.get('unit_count', 'unknown')}")
        on_progress(f"Total SF: {project_quantities.get('total_building_sf', 'unknown')}")
        on_progress(f"Confidence: {project_quantities.get('confidence', 'unknown')}")

        on_progress("Building Excel workbook...")
        excel_bytes = build_excel_bytes(requirements, trades, sov_data=sov_data,
                                        failed_pages=stats.get("failed_pages", []),
                                        project_quantities=project_quantities,
                                        addenda_findings=stats.get("addenda_findings", []))

        job["excel_bytes"] = excel_bytes
        job["stats"] = stats
        job["status"] = "done"

    except Exception as e:
        job["error"] = str(e)
        job["status"] = "error"

    finally:
        q.put(None)  # Sentinel to close SSE stream

        # Clean up uploaded files
        job_dir = job.get("job_dir")
        if job_dir and job_dir.exists():
            shutil.rmtree(job_dir, ignore_errors=True)


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  BuildBrain Web App")
    print("  Local:   http://localhost:5000")
    print("  Network: http://0.0.0.0:5000")
    print("=" * 50)
    app.run(host="0.0.0.0", debug=False, port=5000, threaded=True)
