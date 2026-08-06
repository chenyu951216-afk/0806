# Gate 爆發前掃幣雷達 v3

這是針對「尚未大幅起漲、等待完整突破回踩」的 Gate USDT 小幣掃描與交易追蹤網站。v3 的重點不是增加訊號數，而是把觀察候選與可執行訊號完全分離，並用後端風控拒絕不合格的系統單。

## v3 與舊版的根本差異

- 不再把低分觀察候選當成下單訊號。
- 預設不做突破前提前埋伏；只有已收 15m 突破、後續已收 15m 回踩守住，才進入正式訊號流程。
- OI 只作為情境資料；沒有現貨主動買盤與正 CVD 時，OI 上升不會被視為多頭證據。
- Gate `spot.trades` WebSocket 統計 taker buy ratio 與現貨 CVD。
- Gate `spot.order_book` 連續快照統計買盤持續率、價差及買牆快速消失率；不是只看單次訂單簿。
- 停損依結構低點與 ATR 留出正常波動空間，再由名目金額控制帳戶虧損。
- 固定 Plan 建立後，進場區、SL、TP1、TP2、TP3 不會因再次掃描或實際成交價而改動。
- 正式系統單與手動交易紀錄分開，績效不混用。
- 同時檢查單筆風險、全部未保本持倉總風險、1 倍總名目與持倉相關性。
- 內建保守歷史重播及模擬交易驗證。預設未累積至少 20 筆封閉模擬交易並達到門檻前，正式系統單會被鎖定。

## 訊號生命週期

1. **觀察候選**：基底收斂、未明顯暴漲，但尚無完整進場結構。不可下系統單。
2. **突破等待回踩**：15m 已收線突破，但尚未有後續回踩守住。禁止追突破棒。
3. **正式原始訊號**：突破與回踩都已收線確認，微結構、BTC、市場廣度與本地歷史重播通過。
4. **正式可執行**：原始訊號通過後，還要通過模擬驗證、總風險、1 倍名目和相關性限制，而且即時價格位於固定回踩區。
5. **等待固定回踩價**：型態成立但即時價高於掛單區，只能在固定區掛限價，未成交就放棄。
6. **失效／到期／禁止追價**：不得沿用舊計畫。

## 風控規則

- 單筆目標風險：預設帳戶餘額的 2%。
- 全部未保本持倉總風險：預設最多 3%。
- 1 倍總名目：所有未平倉剩餘名目合計預設最多帳戶餘額的 100%。
- 高相關持倉：預設相關係數達 0.72 時，不允許再新增同方向系統單。
- 系統單成交價必須在固定回踩區內，且不得超過追價上限。
- 手動交易仍可記錄，但會標示為 `MANUAL`，不代表模型推薦。

## 模擬驗證鎖

預設：

- 至少 20 筆封閉模擬交易。
- Profit Factor 至少 1.20。
- 平均期望值至少 0.10R。
- 最大回撤不超過 8R。

只有全部通過，正式系統單按鈕才可能解鎖。剛部署時出現 0 個正式可執行訊號是正常且刻意的安全設計。不要為了快速出訊號關閉驗證鎖；若關閉，代表自行承擔尚未驗證策略的風險。

## Zeabur 部署

1. 將本專案檔案放在 GitHub repository 根目錄。
2. Zeabur 新增 Git Service 並選擇 repository。
3. 貼上 `.env.example` 的環境變數，填入 CoinGlass API Key。
4. HTTP Port 使用 `8080`。
5. 建立持久磁碟並掛載到 `/data`。
6. 部署後先確認 `/health` 正常，再查看首頁的 Gate WS 與模擬驗證狀態。

資料庫仍使用 `/data/scanner_v2.db`，方便從 v2 持久磁碟原地遷移。啟動時會自動補上 v3 欄位，不需要刪除原交易紀錄。

## 完整環境變數

```env
PORT=8080
DATA_DIR=/data
TZ_OFFSET_HOURS=8
AUTO_SCAN=true
DEMO_MODE=false

GATE_BASE_URL=https://api.gateio.ws/api/v4
GATE_WS_URL=wss://api.gateio.ws/ws/v4/
COINGLASS_BASE_URL=https://open-api-v4.coinglass.com
COINGLASS_API_KEY=填入你的CoinGlass付費APIKey
COINGLASS_EXCHANGE=Gate
DISCORD_WEBHOOK_URL=

SCAN_INTERVAL_SECONDS=300
LIVE_PRICE_INTERVAL_SECONDS=5
TRACK_INTERVAL_SECONDS=20
REQUEST_TIMEOUT_SECONDS=15
HTTP_CONCURRENCY=8

MAX_UNIVERSE=120
DEEP_SCAN_LIMIT=55
ORDERBOOK_FINALISTS=18
MAX_SIGNALS=35
COINGLASS_MARKET_PAGES=4
COINGLASS_PER_PAGE=100

MIN_SPOT_QUOTE_VOLUME_USDT=1000000
MIN_FUTURES_QUOTE_VOLUME_USDT=1000000
MIN_MARKET_CAP_USD=12000000
MAX_MARKET_CAP_USD=3500000000
MAX_SPREAD_PCT=0.35
MAX_24H_PUMP_PCT=16
MAX_4H_PUMP_PCT=8
MIN_SETUP_SCORE=64
FORMAL_SIGNAL_SCORE=82
ALERT_SCORE=82
MIN_STOP_PCT=1.4
MAX_STOP_PCT=7
MAX_EXPECTED_MOVE_PCT=55
PLAN_TTL_CANDLES=32

DEFAULT_NOTIONAL_USDT=100
DEFAULT_ACCOUNT_BALANCE_USDT=1000
RISK_PER_TRADE_PCT=2
MAX_TOTAL_OPEN_RISK_PCT=3
MAX_TOTAL_NOTIONAL_PCT=100
MAX_PAIR_CORRELATION=0.72
MAX_CORRELATED_OPEN_POSITIONS=0
CORRELATION_LOOKBACK_CANDLES=96
INTRABAR_POLICY=conservative

ENABLE_EARLY_SWEEP_ENTRY=false
MICROSTRUCTURE_TOP_SYMBOLS=15
MICROSTRUCTURE_WINDOW_SECONDS=900
MICROSTRUCTURE_MIN_SECONDS=180
MICROSTRUCTURE_MIN_TRADES=40
MICROSTRUCTURE_MIN_BOOK_SAMPLES=20
MIN_TAKER_BUY_RATIO=0.55
MIN_BOOK_PERSISTENCE=0.58
MAX_BID_WALL_COLLAPSE_RATE=0.15

PAPER_VALIDATION_REQUIRED=true
MIN_PAPER_TRADES=20
MIN_PAPER_PROFIT_FACTOR=1.20
MIN_PAPER_EXPECTANCY_R=0.10
MAX_PAPER_DRAWDOWN_R=8
MIN_LOCAL_REPLAY_SAMPLES=3
MIN_LOCAL_REPLAY_EXPECTANCY_R=0
MAX_NEW_SYSTEM_SIGNALS_PER_SCAN=1

EXCLUDED_BASES=BTC,ETH,BNB,SOL,XRP,DOGE,ADA,TRX,BCH,LTC,TON,DOT,USDC,USDE,DAI,FDUSD,TUSD,USDD,EUR,USD1,PAXG,XAUT,GT,WBTC,WETH,STETH
LOG_LEVEL=INFO
```

## 使用順序

1. 輸入真實帳戶餘額。
2. 先看「模型驗證」是否通過。
3. 「觀察候選」只觀察，不下單。
4. 正式流程中若顯示「等待回踩」，可將固定回踩區中位價複製到 Gate 作限價參考，但尚未成交前不要按「正式單已成交」。
5. 只有價格成交且網站顯示綠色「正式可執行」時，才按「正式單已成交」。
6. 自己主觀下的單只能用「手動交易紀錄」。

## 重要限制

本專案只提供掃描、計畫、模擬驗證與手動交易追蹤，不會登入 Gate，也不會替你自動下單。任何回測、模擬或即時條件都不能保證未來獲利。小市值幣仍可能受到跳空、滑點、流動性撤退、資料延遲及操縱影響。
