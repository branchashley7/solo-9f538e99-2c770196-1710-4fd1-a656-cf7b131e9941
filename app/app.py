"""潮位补采网页：填写源 URL，启动/继续采集，查看已发布观测并送检质控库。"""
import json
import os
import re

from flask import (Flask, Response, abort, flash, jsonify, redirect,
                   render_template, request, url_for)

from collector import Collector
from submitter import Submitter

DATA_DIR = os.environ.get("APP_DATA", os.path.join(os.path.dirname(__file__), "data"))
SAMPLE_URL = os.environ.get("SAMPLE_SOURCE_URL", "")
QC_BASE_URL = os.environ.get("QC_BASE_URL", "http://qc:9100")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "tide-collector-demo")

collector = Collector(DATA_DIR)
collector.recover_all()  # 启动即共同恢复断点与磁盘字节
submitter = Submitter(collector.store, QC_BASE_URL)
submitter.recover_on_startup()  # 已受理但未拿到回执的送检保持待确认，凭原号恢复

URL_RE = re.compile(r"^https?://\S+$")


def _submission_index():
    """{(url, revision): submission_row} 供页面展示每版送检状态。"""
    idx = {}
    for s in collector.store.list_submissions():
        idx[(s["url"], s["revision"])] = s
    return idx


@app.get("/")
def index():
    jobs = collector.store.list_jobs()
    obs = {j["url"]: collector.store.get_observation(j["url"]) for j in jobs}
    any_running = any(j["status"] == "running" for j in jobs)
    sub_idx = _submission_index()
    return render_template("index.html", jobs=jobs, obs=obs, sub_idx=sub_idx,
                           any_running=any_running, sample_url=SAMPLE_URL,
                           qc_base_url=QC_BASE_URL)


@app.post("/jobs")
def create_and_start():
    url = (request.form.get("url") or "").strip()
    if not URL_RE.match(url):
        flash("请输入合法的 http(s) 源 URL", "error")
        return redirect(url_for("index"))
    job = collector.store.get_or_create_job(url)
    if collector.start_async(job["id"]):
        flash(f"已启动采集任务 #{job['id']}", "ok")
    else:
        flash(f"任务 #{job['id']} 正在运行中", "error")
    return redirect(url_for("index"))


@app.post("/jobs/<int:job_id>/start")
def start(job_id):
    if collector.store.get_job(job_id) is None:
        abort(404)
    if collector.start_async(job_id):
        flash(f"已启动/继续任务 #{job_id}", "ok")
    else:
        flash(f"任务 #{job_id} 正在运行中", "error")
    return redirect(url_for("index"))


@app.post("/jobs/<int:job_id>/reset")
def reset(job_id):
    job = collector.store.get_job(job_id)
    if job is None:
        abort(404)
    if job["status"] == "running":
        flash("任务运行中，不能重置", "error")
        return redirect(url_for("index"))
    collector.reset_job(job_id)  # 舍弃本地前缀与断点；已发布观测不受影响
    collector.store.set_status(job_id, "idle", "已手动重置断点")
    flash(f"任务 #{job_id} 断点已重置", "ok")
    return redirect(url_for("index"))


@app.get("/jobs/<int:job_id>/observation")
def observation(job_id):
    job = collector.store.get_job(job_id)
    if job is None:
        abort(404)
    obs = collector.store.get_observation(job["url"])
    if obs is None:
        flash("该源尚无已发布的完整观测", "error")
        return redirect(url_for("index"))
    rows = [line.split(",", 1) for line in obs["content"].strip().splitlines()]
    return render_template("observation.html", job=job, obs=obs, rows=rows)


@app.get("/jobs/<int:job_id>/observation.csv")
def observation_csv(job_id):
    job = collector.store.get_job(job_id)
    if job is None:
        abort(404)
    obs = collector.store.get_observation(job["url"])
    if obs is None:
        abort(404)
    return Response(obs["content"], mimetype="text/csv",
                    headers={"Content-Disposition":
                             f"attachment; filename=tide_job{job_id}.csv"})


# ---------- 多版原文 ----------
@app.get("/jobs/<int:job_id>/versions")
def versions_page(job_id):
    job = collector.store.get_job(job_id)
    if job is None:
        abort(404)
    versions = collector.store.list_observation_versions(job["url"])
    current = collector.store.get_observation(job["url"])
    sub_idx = _submission_index()
    return render_template("versions.html", job=job, versions=versions,
                           current_rev=current["revision"] if current else None,
                           sub_idx=sub_idx)


@app.get("/jobs/<int:job_id>/versions/<int:revision>/observation.csv")
def version_csv(job_id, revision):
    job = collector.store.get_job(job_id)
    if job is None:
        abort(404)
    ver = collector.store.get_observation_version(job["url"], revision)
    if ver is None:
        abort(404)
    return Response(ver["content"], mimetype="text/csv",
                    headers={"Content-Disposition":
                             f"attachment; filename=tide_job{job_id}_r{revision}.csv"})


# ---------- 送检 ----------
@app.post("/jobs/<int:job_id>/submit")
def submit_job(job_id):
    """送检当前完整观测：固定源 URL、当前版次与完整 CSV。"""
    job = collector.store.get_job(job_id)
    if job is None:
        abort(404)
    try:
        sid, note = submitter.submit_current(job["url"])
        flash(f"送检号 {sid}：{note}", "ok")
    except ValueError as e:
        flash(str(e), "error")
    return redirect(url_for("index"))


@app.get("/submissions")
def submissions_page():
    subs = collector.store.list_submissions()
    # 附带每条登记对应的当前稿版次（当前稿只向新版前进）
    current_rev = {}
    for s in subs:
        if s["url"] not in current_rev:
            obs = collector.store.get_observation(s["url"])
            current_rev[s["url"]] = obs["revision"] if obs else None
    return render_template("submissions.html", submissions=subs,
                           current_rev=current_rev, qc_base_url=QC_BASE_URL)


@app.post("/submissions/<submission_id>/recover")
def recover_one(submission_id):
    """恢复待确认送检：凭原送检号、原登记原文重发/查询。"""
    try:
        results = submitter.recover_pending(submission_id)
        for sid, note in results:
            flash(f"{sid}：{note}", "ok" if "确认" in note or "回执" in note else "error")
    except ValueError as e:
        flash(str(e), "error")
    return redirect(url_for("submissions_page"))


@app.post("/submissions/recover-all")
def recover_all_pending():
    results = submitter.recover_pending()
    if not results:
        flash("没有待确认的送检", "ok")
    for sid, note in results:
        flash(f"{sid}：{note}", "ok" if "确认" in note or "回执" in note else "error")
    return redirect(url_for("submissions_page"))


@app.get("/submissions/<submission_id>/receipt")
def receipt_page(submission_id):
    rec = collector.store.get_submission_by_id(submission_id)
    if rec is None:
        abort(404)
    receipt = json.loads(rec["receipt_json"]) if rec["receipt_json"] else None
    return render_template("receipt.html", rec=rec, receipt=receipt)


@app.get("/api/jobs")
def api_jobs():
    out = []
    for j in collector.store.list_jobs():
        obs = collector.store.get_observation(j["url"])
        d = dict(j)
        d["has_observation"] = obs is not None
        d["observation_etag"] = obs["etag"] if obs else None
        d["observation_rows"] = obs["row_count"] if obs else None
        d["observation_revision"] = obs["revision"] if obs else None
        out.append(d)
    return jsonify(out)


@app.get("/api/submissions")
def api_submissions():
    out = []
    for s in collector.store.list_submissions():
        d = dict(s)
        d.pop("content")  # 列表接口不回显大段原文，单稿可走页面核对
        if d["receipt_json"]:
            d["receipt"] = json.loads(d["receipt_json"])
        out.append(d)
    return jsonify(out)


@app.get("/healthz")
def healthz():
    return "ok"


if __name__ == "__main__":
    port = int(os.environ.get("APP_PORT", "8080"))
    app.run(host="0.0.0.0", port=port, threaded=True)
