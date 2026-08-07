# Gate 10X 多因子自適應雷達 v4.4

v4.4 把策略拆成 Alpha 訊號、執行品質、BTC/市場風險、forward 驗證證據、倉位風控五層。10X、3x、5x、10x 都只是候選與持倉里程碑，不是獲利保證。

## 核心改動

### BTC 不再是普通硬性否決
一般 BTC 回檔、跌破均線或市場廣度偏弱，不再直接把強勢山寨幣全部封鎖。BTC/市場狀態改為 `risk_multiplier`，大約 0.55～1.05 調整真人部位；只有 15m 快速急跌、4h 崩跌或全市場廣度崩潰等真正風險事件才暫停新多單。

### 真人訊號改成多因子加權
舊版要求分數、CVD、主動買盤、訂單簿、BTC、歷史重播幾乎全部同時通過。v4.4 使用 `execution_quality`，把結構分、10X 分、微結構、現貨流、突破/回踩、歷史重播與市場環境一起加權。缺少單一軟條件只會降分；硬拒絕只保留結構失效、極端價差/撤單與市場快速風險事件。

預設真人門檻：EARLY quality 72 / 10X 74；RETEST quality 74 / 10X 72。

### FORMAL 更容易真的累積
正式 forward validation 驗證的是核心策略，不要求真人執行條件先全部完美。EARLY 預設約從 10X 66 / 執行 56 開始；RETEST 約從 64 / 64 開始。每筆仍必須 WAITING_FILL → 後續已收 1m K 真碰價 → OPEN → CLOSED 才算績效。

探索池更寬，只研究漏網訊號，永遠不直接解鎖真人單。

### 證據分級風險
- `BOOTSTRAP`：正式樣本不足，但即時 execution quality ≥ 82，可用最高 0.20% 帳戶風險測試。
- `PROBATION`：至少 4 筆封閉樣本，PF ≥ 1.00、期望值 ≥ 0R、回撤 ≤ 4.5R；最高 0.50%。
- `VERIFIED`：至少 10 筆，PF ≥ 1.10、期望值 ≥ 0.05R、回撤 ≤ 6R；最高 1.25%。
- `FULL`：完整 20 筆 / 每模式至少 8 筆、PF ≥ 1.20、期望值 ≥ 0.10R、回撤 ≤ 8R；可使用網站設定風險，最高 5%。

實際風險還會乘上訊號品質與 BTC risk multiplier，因此設定 5% 不代表每筆自動用滿 5%。

### 限價仍是核心
系統仍以固定限價區為主。價格高於追價線時不市價追，但高品質 EARLY/RETEST 可以把低掛留在固定區等待回踩，不再因現價已高就把整個計畫取消。

## 部署

- Git Service：`chenyu951216-afk/0806` `main`
- Port：`8080`
- Persistent Volume：`/data`
- `.env.example` 完整貼到 Zeabur，再填 CoinGlass API Key
- `/health` 應顯示 `4.4.0`

網站只做掃描、交易計畫、forward paper validation、風控與真人成交回填，不會登入 Gate 自動下單。
