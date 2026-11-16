"""新增单元自测：版次/归档/迁移 + 送检登记 + 质控端幂等指纹。

用法：PYTHONPATH=<flask/requests 所在目录> python3 tests/submit_unit_test.py
"""
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "app"))
sys.path.insert(0, os.path.join(ROOT, "qc"))

import db  # noqa: E402
import qc_app  # noqa: E402

FAILED = []


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + ("" if cond else f" — {detail}"))
    if not cond:
        FAILED.append(name)


def ts(hour=0):
    return datetime(2026, 1, 15, hour, tzinfo=timezone.utc)


# ---------- Store：版次、归档、当前稿只向新版前进 ----------
work = tempfile.mkdtemp(prefix="tide-unit-")
store = db.Store(os.path.join(work, "collector.db"))
url = "http://example/tide.csv"

r1 = store.publish_observation(url, '"e1"', "t,l\n2026-01-15T00:00:00Z,1\n",
                               1, ts(0), ts(1))
check("首次发布为第 1 版", r1 == 1)
r2 = store.publish_observation(url, '"e2"', "t,l\n2026-01-15T00:00:00Z,2\n",
                               1, ts(0), ts(1))
check("同源再次发布版次 +1", r2 == 2)
cur = store.get_observation(url)
check("当前稿指向新版", cur["revision"] == 2 and "2\n" in cur["content"])
versions = store.list_observation_versions(url)
check("两版均归档且版次有序", [v["revision"] for v in versions] == [1, 2])
v1 = store.get_observation_version(url, 1)
check("旧版原文仍可查且未被覆盖", "1\n" in v1["content"] and v1["etag"] == '"e1"')

# 另一个源独立计版
r_other = store.publish_observation("http://other/tide.csv", '"x"',
                                    "t,l\n2026-01-15T00:00:00Z,9\n",
                                    1, ts(0), ts(1))
check("不同源版次各自从 1 开始", r_other == 1)

# ---------- Store：送检登记（先登记后发请求；同版唯一） ----------
reg = store.register_submission(url, 2, cur["content"], '"e2"')
check("送检登记生成 SB- 送检号并待确认",
      reg["submission_id"].startswith("SB-") and reg["status"] == "awaiting_receipt")
try:
    store.register_submission(url, 2, "other", '"e2"')
    check("同版重复登记必须冲突", False)
except sqlite3.IntegrityError:
    check("同版重复登记必须冲突", True)
store.confirm_submission(reg["submission_id"], "RC-1", '{"receipt_id":"RC-1"}')
rec = store.get_submission_for_version(url, 2)
check("确认后保存唯一回执并置 confirmed",
      rec["status"] == "confirmed" and rec["receipt_id"] == "RC-1")
check("待确认列表不含已确认记录",
      all(s["submission_id"] != reg["submission_id"]
          for s in store.list_pending_submissions()))
store.note_submission_error(reg["submission_id"], "x")
check("确认后的记录不会被故障备注改回待确认",
      store.get_submission_by_id(reg["submission_id"])["status"] == "confirmed")

# ---------- 旧库迁移：无 revision 列的 observations 自动补列并归档 ----------
legacy_dir = tempfile.mkdtemp(prefix="tide-legacy-")
legacy_db = os.path.join(legacy_dir, "collector.db")
conn = sqlite3.connect(legacy_db)
conn.executescript("""
CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE,
  etag TEXT, total_size INTEGER DEFAULT -1, confirmed_bytes INTEGER DEFAULT 0,
  status TEXT DEFAULT 'idle', message TEXT DEFAULT '', created_at TEXT, updated_at TEXT);
CREATE TABLE observations (url TEXT PRIMARY KEY, etag TEXT NOT NULL, content TEXT NOT NULL,
  row_count INTEGER NOT NULL, first_ts TEXT NOT NULL, last_ts TEXT NOT NULL, published_at TEXT NOT NULL);
""")
conn.execute("INSERT INTO observations VALUES (?,?,?,?,?,?,?)",
             (url, '"old"', "t,l\n2026-01-15T00:00:00Z,1\n", 1,
              "2026-01-15T00:00:00+00:00", "2026-01-15T00:10:00+00:00", "2026-01-15T00:00:00+00:00"))
conn.commit()
conn.close()
store2 = db.Store(legacy_db)
obs = store2.get_observation(url)
check("迁移后旧稿版次补为 1", obs is not None and obs["revision"] == 1)
check("迁移同时归档第 1 版", store2.get_observation_version(url, 1) is not None)
next_rev = store2.publish_observation(url, '"new"', "t,l\n2026-01-15T00:00:00Z,2\n",
                                      1, ts(0), ts(1))
check("迁移库的下一版为 2（不会与归档冲突）", next_rev == 2)

# ---------- 质控端：指纹、同号同文/异文、唯一回执 ----------
qc_app.DB_PATH = os.path.join(work, "qc.db")
qc_app.init_db()
client = qc_app.app.test_client()
p = {"submission_id": "SB-1", "source_url": url, "revision": 1,
     "content": "t,l\n2026-01-15T00:00:00Z,1\n"}
digest_a = qc_app._fingerprint(url, 1, p["content"])
digest_b = qc_app._fingerprint(url, 1, p["content"] + " ")
check("指纹对内容敏感", digest_a != digest_b)
r = client.post("/submissions", json=p)
check("首次送检 201 且回执唯一", r.status_code == 201
      and r.get_json()["receipt_id"].startswith("RC-"))
receipt1 = r.get_json()["receipt_id"]
r = client.post("/submissions", json=p)
check("同号同文返回 200 原回执", r.status_code == 200
      and r.get_json()["receipt_id"] == receipt1)
r = client.post("/submissions", json=dict(p, content=p["content"] + " "))
check("同号异文 409 拒绝", r.status_code == 409)
r = client.post("/submissions", json=dict(p, submission_id="SB-2"))
check("同源同版不同送检号 409 拒绝", r.status_code == 409)
r = client.post("/submissions",
                json=dict(p, submission_id="SB-2", revision=2))
check("同源新版可独立入库并生成新回执",
      r.status_code == 201 and r.get_json()["receipt_id"] != receipt1)
# 丢回执开关：已入库但 500，关闭后凭原号取回原回执
qc_app.STATE["lose_receipt"] = True
r = client.post("/submissions", json=p)
check("丢回执开关下返回 500", r.status_code == 500)
qc_app.STATE["lose_receipt"] = False
r = client.get("/submissions/SB-1/receipt")
check("关闭开关后凭原号取回原回执",
      r.status_code == 200 and r.get_json()["receipt_id"] == receipt1)

print()
if FAILED:
    print(f"{len(FAILED)} 项失败: {FAILED}")
    sys.exit(1)
print("全部新增单元检查通过")
