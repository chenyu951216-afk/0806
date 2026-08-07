# Gate 雙向多因子自適應雷達 v5.0

這版把 LONG 10X 早期/突破回踩與 SHORT 崩盤前兆/跌破反抽整合成同一套 forward-validation、證據分級與帳戶風控框架。10X、崩盤前兆、3x/5x/10x 都只是模型標籤或里程碑，不是報酬保證。

## v5 修正的實際 BUG

- **同幣重複 paper OPEN**：舊版不同 Plan ID 可能讓同一幣、同方向、同模式同時存在兩筆活躍模擬單。v5 啟動時自動清理舊重複列，並建立資料庫 partial unique index；之後同 `symbol + side + mode + pool` 最多只能有一筆 `WAITING_FILL/OPEN`。
- **真人與模擬混在同頁**：`/api/actionable` 現在只回真人可執行單。FORMAL 模擬單只在 `/api/paper`/正式驗證頁出現，永遠不會因為自己是 OPEN 就自動變真人單。
- **驗證0筆卻看到OPEN很怪**：CLOSED 才是績效樣本。UI 現在明確分開 `WAITING_FILL / OPEN / CLOSED`，四個策略 LONG_EARLY、LONG_RETEST、SHORT_EARLY、SHORT_RETEST 各自顯示。
- **10X頁長期空白**：新增 `/api/radar`。雷達是排序頁，不是進場門檻；即使全部低於重點分數，也會顯示本輪相對最強候選並標示「弱觀察」。
- **Plan漂移造成重複**：LONG/SHORT Plan ID 各自穩定保存，並以價格結構/ATR/區域重疊判斷是否同一計畫。

## LONG：不是條件全部AND

真人 LONG 使用多因子 `execution_quality`：結構、10X輪廓、現貨吸籌、量能、CVD、Taker Buy、訂單簿、OI、Funding、重播與BTC環境一起加權。一般 BTC 弱勢只縮小風險，只有快速 shock/市場廣度崩潰等情況才硬暫停新多單。

v5 預設 LIVE：EARLY Q>=68/10X>=68；RETEST Q>=70/10X>=66。結構失效、極端 spread、追價和帳戶風險仍是硬風控。

## SHORT：獨立崩盤前兆模型

不是把 LONG 公式乘 -1。SHORT 專門評估：

- 派發/上影線與紅量占比；
- lower-high、跌破支撐與反抽拒絕；
- 主動賣出、負 CVD、ask-side depth；
- bid wall 撤退；
- OI 增加但價格轉弱、正 funding、多頭擁擠、OI/市值脆弱；
- BTC偏弱可提高空頭風險倍率，BTC偏強則縮倉；
- 4h/24h 已經暴跌太深會禁止追空。

SHORT 的 SL 在上方、TP 向下；TP1 後停損移成本、TP2 後鎖 +1R、TP3 後剩餘 runner 使用 1h 寬尾隨，保護停損只向下收緊。

## Forward validation 不再卡死

FORMAL 是「收集核心策略證據」，不是先要求真人條件全部完美。除絕對門檻外，v5 新增 **cross-sectional relative cohort**：本輪 Universe 前 30% 的相對強勢/弱勢候選，只要仍高於安全分數地板，可進 FORMAL forward test。這只增加研究樣本，不會直接繞過真人 LIVE 門檻。

每筆仍必須：`WAITING_FILL -> 後續已收1m K真的碰價 -> OPEN -> CLOSED`。沒有碰價不算成交；資料缺口無法完整重建會 `INVALIDATED`。

## 真人風險證據分級

- BOOTSTRAP：樣本不足但即時 Q>=76，最高 0.20%。
- PROBATION：每策略至少4筆且 PF>=1.00、期望>=0R、DD<=4.5R，最高0.50%。
- VERIFIED：至少10筆且 PF>=1.10、期望>=0.05R、DD<=6R，最高1.25%。
- FULL：總FORMAL>=20、每策略至少8筆、PF>=1.20、期望>=0.10R、DD<=8R，才可使用網站設定風險（仍最高5%）。

實際風險還會乘訊號品質與市場 risk multiplier，因此風險設定5%不是每筆都用5%。

## 重要操作區分

- **🚀10X雷達 / 🔻崩盤做空雷達**：只代表相對候選排名。
- **正式驗證池**：後台 forward 模擬單；不是真人掛單。
- **✅真人可下單**：唯一真人執行清單；必須同時滿足目前 LIVE 訊號、證據風險額度、固定價格區與帳戶風控。
- 網站不登入 Gate、不替你送交易所訂單。SHORT 必須由使用者在 Gate 支援做空的合約/保證金產品自行成交後回填網站。

## Zeabur

- Repo：`chenyu951216-afk/0806` branch `main`
- HTTP Port：`8080`
- Persistent Volume：`/data`
- `.env.example` 全部同步到 Zeabur，CoinGlass key/Discord webhook 只放 Zeabur，不進 GitHub。
- 部署完成 `/health` 必須顯示 `5.0.0`。

資料庫沿用 `/data/scanner_v2.db`；既有真人部位與歷史交易保留。舊重複 active paper 會在 v5 migration 時只保留一筆，其餘標記 `INVALIDATED / DUPLICATE_ACTIVE_MIGRATION_V5`，不計入正式績效。
