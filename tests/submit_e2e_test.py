"""送检链路端到端自测：采集端 app + 质控接收端 qc + 示例源 source 三进程。

用法：PYTHONPATH=<flask/requests 所在目录> python3 tests/submit_e2e_test.py

覆盖（对应任务书关键不变式）：
 1. 采集发布 -> 送检 -> 回执 confirmed；接收端库内可核对稿件
 2. 同版重复点击送检：沿用原送检号、原回执，接收端不产生第二条入库
 3. 入库后丢回执（500）：本地保持待确认；关掉开关后凭原送检号恢复，拿到同一张回执
 4. 同号异文：接收端 409 拒绝，库内原文与本地登记均不变
 5. 同源多版交错采集/送检：各版原文与回执均可查，当前稿只向新版前进，版次单调递增
 6. 待确认不妨碍新版采集与其他源送检
 7. 重启后待确认记录不丢，仍凭原号恢复（不能改用后来采到的内容）
 8. 本地登记失败（模拟 SQLite 提交异常）时绝不发出 HTTP 请求
 9. 接收端重启持久化：稿件与回执仍在，同号同文再送返回原回执
10. 网页关键页面可打开（浏览器核对路径）
"""
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
APP_PORT = 18081
QC_PORT = 19100
SRC_PORT = 19001
APP = f"http://127.0.0.1:{APP_PORT}"
QC = f"http://127.0.0.1:{QC_PORT}"
SRC = f"http://127.0.0.1:{SRC_PORT}"
TIDE_URL = f"{SRC}/tide.csv"

FAILED = []


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def http(method, url, data=None, raw=False, timeout=15):
    headers = {}
    body = None
    if data is not None:
        body = data if raw else json.dumps(data).encode()
        headers["Content-Type"] = "application/octet-stream" if raw else "application/json"
    r = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def form(url, fields=None, timeout=15):
    fields = fields or {}
    data = urllib.parse.urlencode(fields).encode()
    r = urllib.request.Request(url, data=data, method="POST",
                               headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


def get(url, timeout=15):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.status, resp.read()


def qc_set_lose(flag):
    form(f"{QC}/control", {"lose_receipt": "on"} if flag else {})


def qc_list():
    _, body = http("GET", f"{QC}/api/submissions")
    return json.loads(body)


def src_set(**kw):
    fields = {"version": kw.get("version", 1), "drop_after": kw.get("drop_after", 0)}
    if kw.get("ignore_range"):
        fields["ignore_range"] = "on"
    form(f"{SRC}/control", fields)


def api_jobs():
    _, body = http("GET", f"{APP}/api/jobs")
    return json.loads(body)


def api_subs():
    _, body = http("GET", f"{APP}/api/submissions")
    return json.loads(body)


def wait_status(url, want, timeout=30):
    deadline = time.time() + timeout
    jid = next(j["id"] for j in api_jobs() if j["url"] == url)
    while time.time() < deadline:
        j = next(x for x in api_jobs() if x["id"] == jid)
        if j["status"] in want:
            return j
        time.sleep(0.2)
    raise TimeoutError(f"{url} 状态未变为 {want}")


def start_collect(url=TIDE_URL):
    form(f"{APP}/jobs", {"url": url})
    return next(j["id"] for j in api_jobs() if j["url"] == url)


def continue_collect(jid):
    form(f"{APP}/jobs/{jid}/start", {})


def submit(jid):
    return form(f"{APP}/jobs/{jid}/submit", {})


def wait_sub_status(submission_id, want, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        recs = [s for s in api_subs() if s["submission_id"] == submission_id]
        if recs and recs[0]["status"] in want:
            return recs[0]
        time.sleep(0.2)
    raise TimeoutError(f"送检 {submission_id} 未进入 {want}")


def wait_port(url, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2):
                return
        except Exception:
            time.sleep(0.3)
    raise TimeoutError(f"{url} 未就绪")


def spawn(cmd, cwd, env):
    return subprocess.Popen([sys.executable] + cmd, cwd=cwd, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def main():
    work = tempfile.mkdtemp(prefix="tide-submit-")
    app_dir = os.path.join(work, "appdata")
    qc_dir = os.path.join(work, "qcdata")
    os.makedirs(app_dir)
    os.makedirs(qc_dir)
    base_env = dict(os.environ)
    if "PYTHONPATH" not in base_env:
        base_env["PYTHONPATH"] = "/tmp/pylibs"

    src = spawn(["source_app.py"], os.path.join(ROOT, "source"),
                dict(base_env, PORT=str(SRC_PORT)))
    qc = spawn(["qc_app.py"], os.path.join(ROOT, "qc"),
               dict(base_env, QC_PORT=str(QC_PORT), QC_DATA=qc_dir, LOSE_RECEIPT="0"))
    app_proc = spawn(["app.py"], os.path.join(ROOT, "app"),
                     dict(base_env, APP_PORT=str(APP_PORT), APP_DATA=app_dir,
                          SAMPLE_SOURCE_URL=TIDE_URL, QC_BASE_URL=QC))
    try:
        wait_port(f"{SRC}/state")
        wait_port(f"{QC}/healthz")
        wait_port(f"{APP}/healthz")
        src_set(version=1)

        # ---- 1. 采集 v1 -> 送检 -> 回执 ----
        jid = start_collect()
        j = wait_status(TIDE_URL, ("done", "failed"))
        check("1.v1 采集发布", j["status"] == "done" and j["observation_revision"] == 1,
              str(j))
        submit(jid)
        subs = api_subs()
        check("1.生成 1 条送检登记", len(subs) == 1, str(subs))
        s1 = subs[0]
        s1 = wait_sub_status(s1["submission_id"], ("confirmed",))
        check("1.送检确认并拿到回执", s1["status"] == "confirmed" and s1["receipt_id"].startswith("RC-"),
              str(s1))
        qc_items = qc_list()
        check("1.接收端库内 1 份稿件", len(qc_items) == 1 and qc_items[0]["revision"] == 1,
              str(qc_items))
        st, body = get(f"{QC}/submissions/{s1['submission_id']}/content")
        check("1.库内原文可核对且与送检一致",
              st == 200 and b"level_m" in body and len(body) > 100)

        # ---- 2. 同版重复送检：沿用原送检号/原回执 ----
        submit(jid)
        subs = api_subs()
        check("2.同版重复送检不新增登记", len(subs) == 1 and subs[0]["submission_id"] == s1["submission_id"])
        check("2.接收端仍只有 1 份稿件", len(qc_list()) == 1)
        check("2.回执号不变", qc_list()[0]["receipt_id"] == s1["receipt_id"])

        # ---- 3. 入库后丢回执 -> 待确认 -> 恢复拿同一张回执 ----
        qc_set_lose(True)
        src_set(version=2)
        continue_collect(jid)
        j = wait_status(TIDE_URL, ("done", "failed"))
        check("3.v2 采集发布为第 2 版", j["status"] == "done" and j["observation_revision"] == 2, str(j))
        submit(jid)
        s2 = next(s for s in api_subs() if s["revision"] == 2)
        s2 = wait_sub_status(s2["submission_id"], ("awaiting_receipt",))
        check("3.丢回执后本地保持待确认", s2["status"] == "awaiting_receipt" and "500" in s2["last_error"],
              str(s2["last_error"]))
        # 接收端其实已 durably 入库
        qc_items = qc_list()
        check("3.稿件已在接收端入库（只是回执丢了）",
              len(qc_items) == 2 and any(i["revision"] == 2 for i in qc_items), str(qc_items))
        qc_set_lose(False)
        form(f"{APP}/submissions/{s2['submission_id']}/recover", {})
        s2b = wait_sub_status(s2["submission_id"], ("confirmed",))
        check("3.凭原送检号恢复并确认", s2b["status"] == "confirmed")
        qc_items = qc_list()
        r2 = next(i for i in qc_items if i["revision"] == 2)
        check("3.恢复后拿到的仍是原回执（同号同文）",
              s2b["receipt_id"] == r2["receipt_id"] and len(qc_items) == 2,
              f"{s2b['receipt_id']} vs {r2['receipt_id']}")

        # ---- 4. 同号异文 -> 409，库内与登记均不变 ----
        bad = {"submission_id": s1["submission_id"], "source_url": TIDE_URL,
               "revision": 1, "content": "utc_time,level_m\n2026-01-15T00:00:00Z,9.99\n"}
        st, body = http("POST", f"{QC}/submissions", bad)
        check("4.同号异文被 409 拒绝", st == 409, body.decode()[:200])
        qc_items = qc_list()
        r1 = next(i for i in qc_items if i["revision"] == 1)
        check("4.库内原文未被改动", r1["submission_id"] == s1["submission_id"]
              and r1["content_sha256"] == next(i for i in qc_items if i["revision"] == 1)["content_sha256"])
        st, body = get(f"{QC}/submissions/{s1['submission_id']}/content")
        check("4.库内原文仍是首次送检内容", b"9.99" not in body)
        check("4.本地原登记仍为 confirmed 且回执不变",
              next(s for s in api_subs() if s["submission_id"] == s1["submission_id"])["receipt_id"]
              == s1["receipt_id"])

        # ---- 5. 多版交错：v2 已送检；再采 v1（新第 3 版）送检，各版均可查 ----
        src_set(version=1)
        continue_collect(jid)
        j = wait_status(TIDE_URL, ("done", "failed"))
        check("5.再次采到 v1 内容发布为第 3 版（版次单调递增）",
              j["observation_revision"] == 3, str(j))
        # 版本归档：3 个版次，当前稿为第 3 版
        st, body = get(f"{APP}/jobs/{jid}/versions")
        check("5.历史版次页面可打开且列 3 版", st == 200 and "第 3 版".encode() in body)
        submit(jid)  # 送检当前第 3 版
        s3 = next(s for s in api_subs() if s["revision"] == 3)
        s3 = wait_sub_status(s3["submission_id"], ("confirmed",))
        check("5.第 3 版送检确认", s3["status"] == "confirmed")
        # 各版原文都可查且互不相同
        _, c1 = get(f"{APP}/jobs/{jid}/versions/1/observation.csv")
        _, c2 = get(f"{APP}/jobs/{jid}/versions/2/observation.csv")
        _, c3 = get(f"{APP}/jobs/{jid}/versions/3/observation.csv")
        check("5.各版原文均归档可查且 r1==r3 内容相同（同源内容重采）、r2 不同",
              c1 == c3 and c2 != c1, f"{len(c1)},{len(c2)},{len(c3)}")
        # 接收端：同源 3 个版次各自一条
        qc_items = qc_list()
        check("5.接收端同源 3 版各自独立入库",
              sorted(i["revision"] for i in qc_items) == [1, 2, 3], str(qc_items))
        # 三条送检各有独立回执
        recv_receipts = {i["revision"]: i["receipt_id"] for i in qc_items}
        check("5.三版回执互不相同且可查",
              len(set(recv_receipts.values())) == 3, str(recv_receipts))

        # ---- 6. 待确认不妨碍新版采集与其他源送检 ----
        qc_set_lose(True)
        src_set(version=2)
        continue_collect(jid)
        j = wait_status(TIDE_URL, ("done", "failed"))
        check("6.第 4 版采集成功（待确认不阻塞采集）", j["observation_revision"] == 4, str(j))
        submit(jid)
        s4 = next(s for s in api_subs() if s["revision"] == 4)
        wait_sub_status(s4["submission_id"], ("awaiting_receipt",))
        # 另一个源（用第二个 URL 指向同一 tide.csv，模拟不同海洋站源）
        other_url = f"{SRC}/tide.csv?station=B"
        jid_b = start_collect(other_url)
        jb = wait_status(other_url, ("done", "failed"))
        check("6.其他源采集不受待确认影响", jb["status"] == "done", str(jb))
        submit(jid_b)
        sb = next(s for s in api_subs() if s["url"] == other_url)
        wait_sub_status(sb["submission_id"], ("awaiting_receipt",))
        check("6.其他源送检登记独立（同样待确认）", sb["status"] == "awaiting_receipt")
        qc_set_lose(False)
        form(f"{APP}/submissions/recover-all", {})
        wait_sub_status(s4["submission_id"], ("confirmed",))
        sb2 = wait_sub_status(sb["submission_id"], ("confirmed",))
        check("6.批量恢复后两条待确认均确认", sb2["status"] == "confirmed"
              and next(s for s in api_subs() if s["submission_id"] == s4["submission_id"])["status"] == "confirmed")

        # ---- 7. 重启 app：待确认登记不丢，凭原号恢复且原文不改 ----
        qc_set_lose(True)
        src_set(version=1)
        continue_collect(jid)
        wait_status(TIDE_URL, ("done", "failed"))
        submit(jid)  # 当前第 5 版，送检后丢回执
        s5 = next(s for s in api_subs() if s["url"] == TIDE_URL and s["revision"] == 5)
        wait_sub_status(s5["submission_id"], ("awaiting_receipt",))
        registered_content = s5  # 记住送检号
        app_proc.send_signal(signal.SIGKILL)
        app_proc.wait()
        app_proc = spawn(["app.py"], os.path.join(ROOT, "app"),
                         dict(base_env, APP_PORT=str(APP_PORT), APP_DATA=app_dir,
                              SAMPLE_SOURCE_URL=TIDE_URL, QC_BASE_URL=QC))
        wait_port(f"{APP}/healthz")
        after = api_subs()
        s5_after = next(s for s in after if s["submission_id"] == s5["submission_id"])
        check("7.重启后待确认登记不丢且仍为原送检号",
              s5_after["status"] == "awaiting_receipt", str(s5_after))
        # 重启后即使当前稿后来又被换版，恢复用的仍是登记原文（库里 content 固定）
        db = sqlite3.connect(os.path.join(app_dir, "collector.db"))
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT content, revision, url FROM submissions WHERE submission_id=?",
                         (s5["submission_id"],)).fetchone()
        db.close()
        check("7.登记原文与版次固定（恢复不会改用后来采到的内容）",
              row["revision"] == 5 and len(row["content"]) > 100)
        qc_set_lose(False)
        form(f"{APP}/submissions/{s5['submission_id']}/recover", {})
        s5b = wait_sub_status(s5["submission_id"], ("confirmed",))
        qc_r5 = next(i for i in qc_list() if i["submission_id"] == s5["submission_id"])
        check("7.重启后凭原号恢复，拿到接收端原回执",
              s5b["receipt_id"] == qc_r5["receipt_id"], f"{s5b['receipt_id']} vs {qc_r5['receipt_id']}")

        # ---- 8. 本地登记失败不发请求 ----
        # 让 submissions 表的提交必然失败：加一个会触发约束/磁盘问题的触发器
        db = sqlite3.connect(os.path.join(app_dir, "collector.db"))
        db.execute("CREATE TRIGGER IF NOT EXISTS fail_register BEFORE INSERT ON submissions "
                   "BEGIN SELECT RAISE(ABORT, 'simulated local register failure'); END")
        db.commit()
        db.close()
        before = len(qc_list())
        # 新源 URL，确保没有历史登记可复用
        fail_url = f"{SRC}/tide.csv?station=C"
        jid_c = start_collect(fail_url)
        wait_status(fail_url, ("done", "failed"))
        submit(jid_c)
        time.sleep(1.0)
        check("8.本地登记失败时无新增送检登记",
              all(s["url"] != fail_url for s in api_subs()))
        check("8.接收端未收到任何请求（入库数不变）", len(qc_list()) == before,
              f"{len(qc_list())} vs {before}")
        # 移除触发器，恢复正常
        db = sqlite3.connect(os.path.join(app_dir, "collector.db"))
        db.execute("DROP TRIGGER IF EXISTS fail_register")
        db.commit()
        db.close()

        # ---- 9. 接收端重启：库内稿件/回执持久化，同号同文返回原回执 ----
        qc.send_signal(signal.SIGTERM)
        qc.wait(timeout=10)
        qc = spawn(["qc_app.py"], os.path.join(ROOT, "qc"),
                   dict(base_env, QC_PORT=str(QC_PORT), QC_DATA=qc_dir, LOSE_RECEIPT="0"))
        wait_port(f"{QC}/healthz")
        items_after = qc_list()
        check("9.接收端重启后稿件与回执仍在", len(items_after) == before,
              f"{len(items_after)} vs {before}")
        st, body = http("POST", f"{QC}/submissions", {
            "submission_id": s1["submission_id"], "source_url": TIDE_URL,
            "revision": 1, "content": c1.decode()})
        check("9.同号同文重送返回 200 原回执",
              st == 200 and json.loads(body)["receipt_id"] == s1["receipt_id"],
              f"status={st} body={body[:200]}")

        # ---- 10. 网页关键页面可打开 ----
        for path in ("/", f"/jobs/{jid}/versions", "/submissions",
                     f"/submissions/{s1['submission_id']}/receipt", "/"):
            st, _ = get(APP + path)
            check(f"10.页面可打开 {path}", st == 200)
        st, body = get(f"{QC}/")
        check("10.质控端库内稿件页面可打开", st == 200 and s1["submission_id"].encode() in body)

        print()
        if FAILED:
            print(f"共 {len(FAILED)} 项失败: {FAILED}")
            return 1
        print("全部送检端到端检查通过")
        return 0
    finally:
        for p in (app_proc, qc, src):
            try:
                p.send_signal(signal.SIGTERM)
                p.wait(timeout=5)
            except Exception:
                p.kill()


if __name__ == "__main__":
    sys.exit(main())
