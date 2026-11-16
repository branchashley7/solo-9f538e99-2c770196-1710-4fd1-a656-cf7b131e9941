"""质控接收端（quality-control receiver）：潮位原文的幂等入库服务。

幂等契约（POST /submissions）：
- 每个送检请求带全局唯一送检号 submission_id、源 URL、版次 revision、完整 CSV 原文。
- 同号 + 同文（url/revision/content 全部一致）：永远返回同一张回执（首次入库或重复送检均如此）。
- 同号 + 异文：拒绝（409），库内既有稿件与回执不受影响。
- 新送检号：校验 CSV 后持久化入库，生成唯一回执并返回（201）。

"入库后丢回执"开关（LOSE_RECEIPT 环境变量默认，控制台可运行时切换）：
- 开启时，稿件已 durably 入库、但响应在送达前"丢失"（返回 500、无回执）。
- 客户端只能凭原送检号重试；重试时同号同文走幂等分支，拿回原回执。

持久化：SQLite（WAL + synchronous=FULL），默认 /data/qc.db，容器内挂命名卷，
重启后库内稿件与回执均不丢。
"""
import hashlib
import os
import sqlite3
import uuid
from datetime import datetime, timezone

from flask import (Flask, Response, abort, jsonify, redirect, render_template,
                   request, url_for)

DATA_DIR = os.environ.get("QC_DATA", os.path.join(os.path.dirname(__file__), "data"))
DB_PATH = os.path.join(DATA_DIR, "qc.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS qc_submissions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    submission_id TEXT NOT NULL UNIQUE,       -- 客户端生成的全局唯一送检号
    source_url    TEXT NOT NULL,
    revision      INTEGER NOT NULL,
    content       TEXT NOT NULL,             -- 完整 CSV 原文
    content_sha256 TEXT NOT NULL,
    row_count     INTEGER NOT NULL,
    receipt_id    TEXT NOT NULL UNIQUE,      -- 接收端生成的唯一回执号
    accepted_at   TEXT NOT NULL,
    UNIQUE(source_url, revision)             -- 同源同版只能入库一份原文
);
"""

app = Flask(__name__)


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _conn():
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def init_db():
    conn = _conn()
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def _lose_receipt_default():
    return os.environ.get("LOSE_RECEIPT", "0").strip().lower() in ("1", "true", "yes", "on")


# 运行时可切换的"入库后丢回执"开关（控制台 /control 修改）
STATE = {"lose_receipt": _lose_receipt_default()}


def _fingerprint(source_url, revision, content):
    h = hashlib.sha256()
    h.update(source_url.encode("utf-8"))
    h.update(b"\x1f")
    h.update(str(revision).encode("ascii"))
    h.update(b"\x1f")
    h.update(content.encode("utf-8"))
    return h.hexdigest()


def _count_rows(content):
    """与采集端一致的行数口径：非空行去掉可选表头后的数据行数。"""
    lines = [ln for ln in content.splitlines() if ln.strip()]
    if not lines:
        return 0
    first = lines[0].split(",")
    looks_like_data = False
    if len(first) >= 2:
        try:
            float(first[1].strip())
            looks_like_data = True
        except ValueError:
            looks_like_data = False
    return len(lines) if looks_like_data else max(0, len(lines) - 1)


def _receipt_payload(row):
    return {
        "receipt_id": row["receipt_id"],
        "submission_id": row["submission_id"],
        "source_url": row["source_url"],
        "revision": row["revision"],
        "content_sha256": row["content_sha256"],
        "row_count": row["row_count"],
        "accepted_at": row["accepted_at"],
        "status": "accepted",
    }


@app.post("/submissions")
def submit():
    """幂等送检入口。接受 JSON 或表单。"""
    if request.is_json:
        payload = request.get_json(silent=True) or {}
    else:
        payload = request.form.to_dict()

    submission_id = (payload.get("submission_id") or "").strip()
    source_url = (payload.get("source_url") or "").strip()
    content = payload.get("content") or ""
    try:
        revision = int(payload.get("revision"))
    except (TypeError, ValueError):
        return jsonify({"error": "revision 必须为整数"}), 400

    if not submission_id or not source_url or not content:
        return jsonify({"error": "submission_id / source_url / content 均不能为空"}), 400
    if revision < 1:
        return jsonify({"error": "revision 必须 >= 1"}), 400
    if not (source_url.startswith("http://") or source_url.startswith("https://")):
        return jsonify({"error": "source_url 必须是 http(s) URL"}), 400

    digest = _fingerprint(source_url, revision, content)

    conn = _conn()
    try:
        existing = conn.execute(
            "SELECT * FROM qc_submissions WHERE submission_id=?",
            (submission_id,)).fetchone()
        if existing is not None:
            # 幂等分支：同号必须同文，否则拒绝且库内稿件不变
            if existing["content_sha256"] != digest:
                return jsonify({
                    "error": "同送检号但原文不一致，拒绝送检",
                    "submission_id": submission_id,
                    "stored_sha256": existing["content_sha256"],
                    "incoming_sha256": digest,
                }), 409
            response_status = 200  # 同号同文：返回原回执
            row = existing
        else:
            # 新送检号：同源同版只接受一份原文（后来者冲突拒绝）
            dup = conn.execute(
                "SELECT * FROM qc_submissions WHERE source_url=? AND revision=?",
                (source_url, revision)).fetchone()
            if dup is not None:
                return jsonify({
                    "error": "该源该版次已有不同送检号的稿件，拒绝重复入库",
                    "source_url": source_url,
                    "revision": revision,
                    "existing_submission_id": dup["submission_id"],
                }), 409
            receipt_id = "RC-" + uuid.uuid4().hex
            row_data = {
                "submission_id": submission_id,
                "source_url": source_url,
                "revision": revision,
                "content": content,
                "content_sha256": digest,
                "row_count": _count_rows(content),
                "receipt_id": receipt_id,
                "accepted_at": _now(),
            }
            conn.execute(
                "INSERT INTO qc_submissions"
                " (submission_id, source_url, revision, content, content_sha256,"
                "  row_count, receipt_id, accepted_at)"
                " VALUES (:submission_id,:source_url,:revision,:content,:content_sha256,"
                "         :row_count,:receipt_id,:accepted_at)",
                row_data)
            conn.commit()  # 回执返回前稿件已 durably 落盘
            response_status = 201
            row = conn.execute(
                "SELECT * FROM qc_submissions WHERE submission_id=?",
                (submission_id,)).fetchone()
    finally:
        conn.close()

    # 入库后丢回执：稿件已落盘，但客户端拿不到响应（模拟响应丢失）。
    # 客户端必须凭原送检号重试，届时走同号同文幂等分支取回原回执。
    if STATE["lose_receipt"]:
        return jsonify({
            "error": "模拟故障：稿件已入库，但回执在送达前丢失",
            "submission_id": submission_id,
        }), 500

    return jsonify(_receipt_payload(row)), response_status


@app.get("/submissions/<submission_id>/receipt")
def get_receipt(submission_id):
    """凭原送检号查询回执（待确认恢复也走同一幂等语义）。"""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM qc_submissions WHERE submission_id=?",
            (submission_id,)).fetchone()
    finally:
        conn.close()
    if row is None:
        return jsonify({"error": "未找到该送检号的记录",
                        "submission_id": submission_id}), 404
    if STATE["lose_receipt"]:
        # 开关开启时查询也"丢回执"，迫使客户端在故障恢复后再凭原号取回
        return jsonify({"error": "模拟故障：回执查询响应丢失",
                        "submission_id": submission_id}), 500
    return jsonify(_receipt_payload(row))


@app.get("/submissions/<submission_id>/content")
def get_content(submission_id):
    """取回某送检号入库的原文（浏览器核对库内稿件用）。"""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM qc_submissions WHERE submission_id=?",
            (submission_id,)).fetchone()
    finally:
        conn.close()
    if row is None:
        abort(404)
    fname = f"qc_{row['source_url'].split('//')[-1].replace('/', '_')}_r{row['revision']}.csv"
    return Response(row["content"], mimetype="text/csv",
                    headers={"Content-Disposition": f"inline; filename={fname}"})


@app.get("/api/submissions")
def api_list():
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT submission_id, source_url, revision, content_sha256,"
            " row_count, receipt_id, accepted_at"
            " FROM qc_submissions ORDER BY accepted_at, id").fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])


@app.get("/")
def index_page():
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT submission_id, source_url, revision, content_sha256,"
            " row_count, receipt_id, accepted_at, length(content) AS content_bytes"
            " FROM qc_submissions ORDER BY id").fetchall()
        items = [dict(r) for r in rows]
    finally:
        conn.close()
    return render_template("index.html", items=items,
                           lose_receipt=STATE["lose_receipt"])


@app.post("/control")
def control():
    STATE["lose_receipt"] = "lose_receipt" in request.form
    if request.accept_mimetypes.best == "application/json":
        return jsonify(STATE)
    return redirect(url_for("index_page"))


@app.get("/healthz")
def healthz():
    return "ok"


init_db()


if __name__ == "__main__":
    port = int(os.environ.get("QC_PORT", "9100"))
    app.run(host="0.0.0.0", port=port, threaded=True)
