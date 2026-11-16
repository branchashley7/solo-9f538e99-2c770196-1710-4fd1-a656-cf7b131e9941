# 潮位补采台（tide-collector）+ 质控送检

海洋站数据员补采单日潮位 CSV 的小系统：网页填写源 URL，一键启动/继续采集；
断网或进程重启后从已确认字节续传，整份到齐且通过 CSV 校验才替换网页中的
可用观测，失败时仍提供上一份完整数据。采集到的完整观测可**送检质控库**：
同源每次成功替换递增版次，送检固定“源 URL + 版次 + 完整 CSV”，同版重复
送检沿用原送检号；质控接收端幂等出回执（同号同文返回原回执、异文拒绝），
入库后丢回执时本地标“待确认”，重启仍凭原号恢复，绝不改用后来采到的内容。

## 组成

| 目录 | 说明 |
|---|---|
| `app/` | Flask + SQLite 采集与送检网页（默认 8080 端口） |
| `qc/` | 质控接收端（默认 9100 端口）：幂等入库、唯一回执、库内稿件浏览、入库后丢回执开关 |
| `source/` | 本地示例潮位源（默认 9000 端口），强 ETag + Range，可切换版本/断流/忽略 Range/错误 206 |
| `tests/` | 单元测试与端到端自测（采集 + 送检两条链路） |
| `docker-compose.yml` | 一键拉起 app + qc + source，全部数据走命名卷持久化 |

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- 采集/送检网页：http://localhost:8080 （表单预填示例源 `http://source:9000/tide.csv`）
- 质控接收端（库内稿件核对）：http://localhost:9100
- 示例源控制台：http://localhost:9000 （切换版本、断流字节数、忽略 Range、错误 206）

持久化卷：`appdata`（断点 + .part + 观测多版归档 + 送检登记）、
`qcdata`（质控库稿件 + 回执）；`docker compose restart` 后采集断点、
待确认送检与质控库稿件均不丢。

“入库后丢回执”开关：qc 服务的 `LOSE_RECEIPT` 环境变量（默认 `0`），
也可在 http://localhost:9100 首页运行时勾选。开启后稿件仍 durably 入库，
但送检/查回执响应返回 500，演示客户端凭原送检号重试取回原回执。

## 本地运行（无 Docker）

```bash
pip install -r app/requirements.txt -r qc/requirements.txt -r source/requirements.txt
python3 source/source_app.py &                 # 9000 端口
QC_PORT=9100 python3 qc/qc_app.py &            # 9100 端口
APP_PORT=8080 SAMPLE_SOURCE_URL=http://127.0.0.1:9000/tide.csv \
  QC_BASE_URL=http://127.0.0.1:9100 python3 app/app.py
```

## 自测

```bash
python3 tests/unit_test.py        # CSV 校验 / Content-Range 解析（原采集单测）
python3 tests/submit_unit_test.py # 版次/归档/旧库迁移/送检登记/QC 幂等指纹
python3 tests/e2e_test.py         # 采集 8 组端到端场景
python3 tests/submit_e2e_test.py  # 送检 10 组端到端场景（39 项检查）
```

## 送检与质控规则（submitter.py / qc_app.py 的不变式）

1. **先登记后发请求**：送检先在本地 SQLite 提交一条登记（含送检号、源 URL、
   版次、完整原文），登记事务失败绝不发出 HTTP 请求。
2. **版次单调递增**：同源每次成功替换观测，版次 +1（`observation_versions`
   归档全部历史版次，`observations` 只存当前稿），当前稿只向新版前进，
   旧稿不会覆盖新稿；不同源各自从第 1 版计。
3. **同版沿用原送检号**：`submissions (url, revision)` 唯一，重复点击送检
   不会生成第二个送检号；已确认则直接看回执，待确认则走恢复。
4. **接收端幂等**：同送检号且同文（url/revision/content 指纹一致）永远返回
   同一张回执（首次 201，重复 200）；同号异文返回 409 拒绝，库内稿件与本地
   登记均不变；同源同版不允许第二个送检号入库。
5. **未取得回执即待确认**：超时、连接错误、500（丢回执开关）等任何未拿到
   明确回执的情况，本地状态保持 `awaiting_receipt` 并记录故障原因；
   接收端稿件可能已入库，恢复时凭原送检号、用**登记时的原文**重试，
   不能改用后来采到的内容。
6. **重启不丢**：登记在 WAL + `synchronous=FULL` 的 SQLite 中，进程重启后
   待确认记录仍在，网页“恢复待确认送检”（单条或全部）即可凭原号取回原回执。
7. **互不阻塞**：待确认不妨碍新版采集，也不妨碍其他源送检；每条送检登记
   独立，发送锁按送检号串行。
8. **多版可查**：采集端“历史版次”页可查同源每版原文与对应送检状态/回执；
   质控端首页列出全部库内稿件，可在线核对原文与回执 JSON。

## 采集与续传规则（collector.py 的不变式）


1. `.part` 文件只追加；每写一块执行 `write → fsync → 提交 SQLite 断点`，
   断点（`confirmed_bytes`）与磁盘字节共同推进。
2. 进程启动时恢复：`confirmed = min(磁盘字节, 断点)`，文件截到 `confirmed`，
   断点同步改回该值 —— 二者一起恢复，不跳字节；`.part` 丢失则整体清零重收。
3. 同版续传：请求带 `Range: bytes=N-` 与 `If-Range: <etag>`，不重取前缀。
4. 换版（HEAD 预检 ETag 变化、206 上 ETag 不符、416）或续传返回 200
   （源忽略 Range）→ 舍弃旧前缀整体重收，绝不混合两份文件。
5. 206 必须核对：`Content-Range` 起点 == 断点、总长与已知一致、
   `Content-Length` 与区间长度一致；任何异常响应不写盘、不推进断点。
6. 整份到齐（`confirmed == total`）且通过 CSV 校验才在一个事务里
   `INSERT OR REPLACE` 发布观测；校验失败或下载中断时旧观测保持可读。
7. 连接提前断开按“应得字节 vs 实得字节”核对（读取前关闭 urllib3 的
   `enforce_content_length`，自己判定短读），已写字节安全保留，任务置为
   `interrupted` 等待继续。

## CSV 校验规则（csvvalid.py）

- 两列：UTC 时刻（ISO 8601，须带时区，`Z` 或偏移均可，归一到 UTC）+ 潮位；
- 潮位须为有限数值（拒绝 NaN/inf/非数值）；
- 时刻严格递增（等值或倒退均拒绝）；
- 首行可为表头（自动识别，无表头时首行不丢）。

## 示例源开关（source/，控制台在 http://localhost:9000）

- **版本 1 / 2**：两份不同但合法的潮位 CSV（144 / 150 行），ETag 随内容变；
- **版本 3**：损坏版（NaN、时刻倒退），用于演示校验失败不替换观测；
- **断流**：每次响应送出 N 字节后提前关闭连接（Content-Length 未满足）；
- **忽略 Range**：对续传请求一律回 200 全量；
- **错误 206**：`Content-Range` 起点故意错位，用于演示客户端核对；
- `/stats` 返回每次请求的方法/Range/状态/实际送出字节，供核对续传行为。

## 已知限制

- 源必须提供强 ETag 与 Content-Length 才能安全续传；无 Content-Length 的
  200 响应按“干净读完即完整”处理（提前断开无法识别），示例源始终带长度。
- 会话内不自动重试：断流后任务置 `interrupted`，需再次点击“启动/继续”
  （符合补采场景人工确认的习惯）。
- 使用 Flask 开发服务器（threaded），面向单站 QC 演示；生产部署应换
  WSGI 服务器并加访问控制。
- 每块写盘都 fsync，吞吐优先的场景可调大块大小（`collector.CHUNK`）。
