"""送检客户端：把固定版次的完整原文幂等送到质控接收端。

不变式（任何时刻、包括进程崩溃后都成立）：
1. 先本地登记（Store.register_submission，已 commit/fsync）再发 HTTP；
   本地登记失败绝不发出请求。
2. 送检内容三元组固定：源 URL + 版次 + 该版完整 CSV 原文；同版重复送检
   沿用同一个送检号（submissions 表 (url,revision) 唯一）。
3. 同号同文：接收端返回原回执 -> 置 confirmed；同号异文：接收端 409 ->
   标记 last_error 且本地登记/原文/状态一律不变（仍 awaiting_receipt，
   因为这是协议冲突，需要人工核对，绝不自动改用新内容）。
4. 任何未拿到明确回执的故障（超时、连接错误、500 丢回执开关等）：
   状态保持 awaiting_receipt（待确认），重启后凭原送检号恢复，
   重试内容仍是登记时的原文，不能改用后来采到的内容。
5. 恢复只查询/重发原登记记录，与采集流程解耦：待确认不妨碍新版采集
   及其他源送检（每条登记独立，发送锁按提交记录串行但互不阻塞）。
"""
import json
import threading

import requests

TIMEOUT = (5, 30)  # (连接, 读取) 秒


class Submitter:
    def __init__(self, store, qc_base_url):
        self.store = store
        self.qc_base_url = qc_base_url.rstrip("/")
        self._locks = {}
        self._guard = threading.Lock()

    def _lock_for(self, submission_id):
        with self._guard:
            return self._locks.setdefault(submission_id, threading.Lock())

    # ---------- 网页入口 ----------
    def submit_current(self, url):
        """送检某源当前稿。返回 (submission_id, note)；无法送检时抛 ValueError。"""
        obs = self.store.get_observation(url)
        if obs is None:
            raise ValueError("该源尚无完整观测，不能送检")
        revision = obs["revision"]
        # 同版已有登记 -> 沿用原送检号（无论 confirmed 还是 awaiting_receipt）
        existing = self.store.get_submission_for_version(url, revision)
        if existing is not None:
            note = ("该版已送检，沿用原送检号"
                    + ("（已确认，可查回执）" if existing["status"] == "confirmed"
                       else "（待确认，可恢复）"))
            return existing["submission_id"], note

        # 先本地登记；登记事务失败会抛异常，此时绝不发请求
        reg = self.store.register_submission(
            url, revision, obs["content"], obs["etag"])
        note = self._send(reg["submission_id"])
        return reg["submission_id"], note

    def recover_pending(self, submission_id=None):
        """恢复待确认送检：凭原送检号、用登记时的原文重试。

        submission_id 为 None 时恢复全部待确认记录。返回处理摘要列表。
        """
        if submission_id is not None:
            rec = self.store.get_submission_by_id(submission_id)
            if rec is None:
                raise ValueError("送检记录不存在")
            if rec["status"] == "confirmed":
                return [(submission_id, "已是确认状态，无需恢复")]
            pending = [rec]
        else:
            pending = self.store.list_pending_submissions()
        results = []
        for rec in pending:
            results.append((rec["submission_id"], self._send(rec["submission_id"])))
        return results

    def recover_on_startup(self):
        """进程启动时把残留待确认记录的故障原因刷新（不自动发请求，
        由数据员在网页点“恢复待确认送检”；登记本身已在持久化库中不丢）。"""
        for rec in self.store.list_pending_submissions():
            if not rec["last_error"]:
                self.store.note_submission_error(
                    rec["submission_id"], "进程重启前未取得回执，待人工恢复")

    # ---------- 核心发送 ----------
    def _send(self, submission_id):
        """按登记内容发送一次。线程串行化同一送检号，返回给数据员的提示语。

        只有拿到接收端明确回执（200/201）才改状态为 confirmed；
        其余一切情况状态保持 awaiting_receipt。
        """
        lock = self._lock_for(submission_id)
        if not lock.acquire(blocking=False):
            return "该送检正在进行中，请稍候查看结果"
        try:
            rec = self.store.get_submission_by_id(submission_id)
            if rec is None:
                return "送检登记已不存在"
            if rec["status"] == "confirmed":
                return f"已确认，回执号 {rec['receipt_id']}"

            payload = {
                "submission_id": rec["submission_id"],
                "source_url": rec["url"],
                "revision": rec["revision"],
                "content": rec["content"],  # 始终用登记原文，不读当前稿
            }
            try:
                resp = requests.post(
                    f"{self.qc_base_url}/submissions", json=payload, timeout=TIMEOUT)
            except requests.RequestException as e:
                # 未取得回执：可能已入库也可能未入库，标待确认，凭原号恢复
                self.store.note_submission_error(
                    submission_id, f"请求失败（待确认）: {e}")
                return f"未取得回执，已标待确认：{e}"

            if resp.status_code in (200, 201):
                try:
                    receipt = resp.json()
                except ValueError:
                    self.store.note_submission_error(
                        submission_id, "回执不是合法 JSON（待确认）")
                    return "回执解析失败，已保持待确认"
                receipt_id = receipt.get("receipt_id")
                if not receipt_id or receipt.get("submission_id") != submission_id:
                    self.store.note_submission_error(
                        submission_id, "回执字段缺失或送检号不符（待确认）")
                    return "回执内容不符，已保持待确认"
                # 原文、唯一回执与当前稿共同提交后的最终确认一步落盘
                self.store.confirm_submission(
                    submission_id, receipt_id, json.dumps(receipt, ensure_ascii=False))
                return f"已确认，回执号 {receipt_id}"

            if resp.status_code == 409:
                # 同号异文（或同源同版被别的送检号占用）：拒绝。登记与原文不变。
                self.store.note_submission_error(
                    submission_id, f"接收端拒绝（409 冲突）: {resp.text[:300]}")
                return "接收端以 409 拒绝（同号异文冲突），登记保持不变，请核对原文"

            # 500（入库后丢回执开关）等：稿件可能已入库，标待确认、凭原号恢复
            self.store.note_submission_error(
                submission_id,
                f"接收端返回 {resp.status_code}（待确认）: {resp.text[:300]}")
            return (f"接收端返回 {resp.status_code}，无法确认是否入库，"
                    "已保持待确认，可凭原送检号恢复")
        finally:
            lock.release()
            with self._guard:
                # 锁表只保留仍待确认的记录，避免长期运行无限增长
                rec = self.store.get_submission_by_id(submission_id)
                if rec is not None and rec["status"] == "confirmed":
                    self._locks.pop(submission_id, None)
