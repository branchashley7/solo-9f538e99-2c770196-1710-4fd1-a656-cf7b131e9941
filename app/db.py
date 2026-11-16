"""SQLite 存储层：采集任务断点 + 已发布观测（多版归档）+ 送检登记。

设计要点：
- jobs 表保存断点状态（etag / total_size / confirmed_bytes），与磁盘上的
  .part 文件共同构成可恢复状态；两者在 collector.recover_all() 中一起核对。
- observations 表只保存“当前稿”：整份到齐且通过 CSV 校验才在一个事务里替换，
  读者永远看到上一份完整数据；每次同源成功替换版次 revision 递增。
- observation_versions 表归档同源所有历史版次原文，多版交错送检时各版均可查；
  当前稿只向新版前进（revision 单调递增，旧稿不会覆盖新稿）。
- submissions 表是送检的本地登记（durable outbox）：先本地提交再发请求，
  本地登记失败绝不发请求；重启后 awaiting_receipt 记录凭原送检号即可恢复，
  绝不改用后来采到的内容。
"""
import os
import sqlite3
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT NOT NULL UNIQUE,
    etag            TEXT,                    -- 当前正在接收版本的强 ETag
    total_size      INTEGER NOT NULL DEFAULT -1,  -- 期望总字节数，-1 未知
    confirmed_bytes INTEGER NOT NULL DEFAULT 0,   -- 已确认落盘字节数
    status          TEXT NOT NULL DEFAULT 'idle', -- idle/running/interrupted/failed/done
    message         TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
    url          TEXT PRIMARY KEY,
    etag         TEXT NOT NULL,
    revision     INTEGER NOT NULL DEFAULT 1, -- 当前稿版次，同源每次替换 +1
    content      TEXT NOT NULL,              -- 完整且通过校验的 CSV 正文
    row_count    INTEGER NOT NULL,
    first_ts     TEXT NOT NULL,
    last_ts      TEXT NOT NULL,
    published_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observation_versions (
    url          TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    etag         TEXT NOT NULL,
    content      TEXT NOT NULL,
    row_count    INTEGER NOT NULL,
    first_ts     TEXT NOT NULL,
    last_ts      TEXT NOT NULL,
    published_at TEXT NOT NULL,
    PRIMARY KEY (url, revision)
);
CREATE TABLE IF NOT EXISTS submissions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    submission_id  TEXT NOT NULL UNIQUE,     -- 全局唯一送检号（UUID）
    url            TEXT NOT NULL,
    revision       INTEGER NOT NULL,         -- 送检时固定的版次
    content        TEXT NOT NULL,            -- 送检时固定的完整 CSV 原文
    etag           TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'awaiting_receipt',
                                             -- awaiting_receipt（待确认）/ confirmed（已确认）
    receipt_id     TEXT,                     -- 接收端唯一回执号
    receipt_json   TEXT,                     -- 完整回执（JSON 文本）
    last_error     TEXT NOT NULL DEFAULT '',
    registered_at  TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    UNIQUE (url, revision)                   -- 同版重复送检沿用原送检号
);
"""

# 每次操作独立连接（WAL 模式下读写不互斥），避免跨线程共享连接。


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Store:
    def __init__(self, db_path):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        conn = self._conn()
        conn.executescript(SCHEMA)
        self._migrate(conn)
        conn.commit()
        conn.close()

    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")  # 提交即落盘，断点/登记不丢
        return conn

    def _migrate(self, conn):
        """旧库升级：observations 补 revision 列，既有稿件视为第 1 版并归档。"""
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(observations)")}
        if "revision" not in cols:
            conn.execute("ALTER TABLE observations ADD COLUMN revision INTEGER NOT NULL DEFAULT 1")
            rows = conn.execute("SELECT * FROM observations").fetchall()
            for r in rows:
                conn.execute(
                    "INSERT OR IGNORE INTO observation_versions"
                    " (url, revision, etag, content, row_count, first_ts, last_ts, published_at)"
                    " VALUES (?,?,?,?,?,?,?,?)",
                    (r["url"], 1, r["etag"], r["content"], r["row_count"],
                     r["first_ts"], r["last_ts"], r["published_at"]))

    # ---------- jobs ----------
    def get_or_create_job(self, url):
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM jobs WHERE url=?", (url,)).fetchone()
            if row is None:
                now = _now()
                cur = conn.execute(
                    "INSERT INTO jobs (url, created_at, updated_at) VALUES (?,?,?)",
                    (url, now, now),
                )
                conn.commit()
                row = conn.execute("SELECT * FROM jobs WHERE id=?",
                                   (cur.lastrowid,)).fetchone()
            return dict(row)
        finally:
            conn.close()

    def get_job(self, job_id):
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_jobs(self):
        conn = self._conn()
        try:
            rows = conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def update_checkpoint(self, job_id, confirmed_bytes, total_size=None, etag=None):
        """推进断点。调用方必须先保证对应字节已 fsync 落盘。"""
        conn = self._conn()
        try:
            sets = ["confirmed_bytes=?", "updated_at=?"]
            args = [confirmed_bytes, _now()]
            if total_size is not None:
                sets.append("total_size=?")
                args.append(total_size)
            if etag is not None:
                sets.append("etag=?")
                args.append(etag)
            args.append(job_id)
            conn.execute(f"UPDATE jobs SET {', '.join(sets)} WHERE id=?", args)
            conn.commit()
        finally:
            conn.close()

    def reset_checkpoint(self, job_id):
        """舍弃旧前缀：断点清零、清空版本信息（换版或需整体重收时调用）。"""
        conn = self._conn()
        try:
            conn.execute(
                "UPDATE jobs SET etag=NULL, total_size=-1, confirmed_bytes=0,"
                " updated_at=? WHERE id=?",
                (_now(), job_id),
            )
            conn.commit()
        finally:
            conn.close()

    def set_status(self, job_id, status, message=""):
        conn = self._conn()
        try:
            conn.execute(
                "UPDATE jobs SET status=?, message=?, updated_at=? WHERE id=?",
                (status, message, _now(), job_id),
            )
            conn.commit()
        finally:
            conn.close()

    # ---------- observations（当前稿 + 多版归档） ----------
    def next_revision(self, conn, url):
        row = conn.execute(
            "SELECT COALESCE(MAX(revision),0) AS m FROM observation_versions WHERE url=?",
            (url,)).fetchone()
        return row["m"] + 1

    def publish_observation(self, url, etag, content, row_count, first_ts, last_ts):
        """替换当前稿并归档新版（单事务）：版次单调递增，旧稿不覆盖新稿。

        返回新版本号。读者要么看到完整的旧稿、要么看到完整的新稿。
        first_ts/last_ts 接受 datetime 或已是 ISO 字符串。
        """
        first_s = getattr(first_ts, "isoformat", None)
        first_s = first_s() if callable(first_s) else first_ts
        last_s = getattr(last_ts, "isoformat", None)
        last_s = last_s() if callable(last_s) else last_ts
        conn = self._conn()
        try:
            revision = self.next_revision(conn, url)
            now = _now()
            conn.execute(
                "INSERT INTO observation_versions"
                " (url, revision, etag, content, row_count, first_ts, last_ts, published_at)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (url, revision, etag, content, row_count, first_s, last_s, now))
            conn.execute(
                "INSERT INTO observations"
                " (url, etag, revision, content, row_count, first_ts, last_ts, published_at)"
                " VALUES (?,?,?,?,?,?,?,?)"
                " ON CONFLICT(url) DO UPDATE SET"
                " etag=excluded.etag, revision=excluded.revision,"
                " content=excluded.content, row_count=excluded.row_count,"
                " first_ts=excluded.first_ts, last_ts=excluded.last_ts,"
                " published_at=excluded.published_at",
                (url, etag, revision, content, row_count, first_s, last_s, now))
            conn.commit()
            return revision
        finally:
            conn.close()

    def get_observation(self, url):
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM observations WHERE url=?", (url,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_observation_version(self, url, revision):
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM observation_versions WHERE url=? AND revision=?",
                (url, revision)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_observation_versions(self, url):
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT url, revision, etag, row_count, first_ts, last_ts, published_at"
                " FROM observation_versions WHERE url=? ORDER BY revision",
                (url,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ---------- submissions（送检登记，durable outbox） ----------
    def register_submission(self, url, revision, content, etag):
        """本地登记送检并提交（成功返回记录）。调用方必须在本方法成功后才发 HTTP。

        同 (url, revision) 已登记时抛 sqlite3.IntegrityError，由调用方改为
        沿用原送检号，绝不生成第二个号。
        """
        import uuid
        conn = self._conn()
        try:
            now = _now()
            submission_id = "SB-" + uuid.uuid4().hex
            conn.execute(
                "INSERT INTO submissions"
                " (submission_id, url, revision, content, etag, status,"
                "  registered_at, updated_at)"
                " VALUES (?,?,?,?,?, 'awaiting_receipt', ?, ?)",
                (submission_id, url, revision, content, etag, now, now))
            conn.commit()
        finally:
            conn.close()
        return self.get_submission_by_id(submission_id)

    def get_submission_by_id(self, submission_id):
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM submissions WHERE submission_id=?",
                (submission_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_submission_for_version(self, url, revision):
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM submissions WHERE url=? AND revision=?",
                (url, revision)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_submissions(self):
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM submissions ORDER BY id").fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def list_pending_submissions(self):
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM submissions WHERE status='awaiting_receipt' ORDER BY id"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def confirm_submission(self, submission_id, receipt_id, receipt_json):
        """拿到回执：置 confirmed 并保存唯一回执。失败（异常）时不调用、状态不变。"""
        conn = self._conn()
        try:
            conn.execute(
                "UPDATE submissions SET status='confirmed', receipt_id=?,"
                " receipt_json=?, last_error='', updated_at=? WHERE submission_id=?",
                (receipt_id, receipt_json, _now(), submission_id))
            conn.commit()
        finally:
            conn.close()

    def note_submission_error(self, submission_id, message):
        """记录一次未确认的失败原因；状态保持 awaiting_receipt，登记与原文不变。"""
        conn = self._conn()
        try:
            conn.execute(
                "UPDATE submissions SET last_error=?, updated_at=? WHERE submission_id=?",
                (message[:500], _now(), submission_id))
            conn.commit()
        finally:
            conn.close()
