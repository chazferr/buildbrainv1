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

from engine import build_excel_bytes, process_files, parse_sov_from_buyout, SUPPORTED_EXTENSIONS

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
                msg = q.get(timeout=60)
            except Empty:
                # Send keepalive
                yield ":\n\n"
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

        on_progress("Building Excel workbook...")
        excel_bytes = build_excel_bytes(requirements, trades, sov_data=sov_data)

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
