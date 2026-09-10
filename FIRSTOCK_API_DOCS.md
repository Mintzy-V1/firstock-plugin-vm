# Firstock API Reference

Base URL: `https://api.firstock.in/V1`
WebSocket: `wss://socket.firstock.in/V2/ws`

Every authenticated call is a `POST` with `Content-Type: application/json` carrying
`{userId, jKey}` in the body, where `jKey` is the `susertoken` returned by `/login`.

---

## Response Format

### Success

```json
{
  "status": "success",
  "message": "Operation successful",
  "data": {}
}
```

### Failure

```json
{
  "status": "failed",
  "code": "400",
  "name": "BAD_REQUEST",
  "error": {
    "field": "exchange",
    "message": "required field is empty or missing: exchange"
  }
}
```

### Common Codes

| Code | Name | HTTP | Meaning |
|---|---|---|---|
| 200 | N/A (Success) | 200 | Request succeeded |
| 400 | BAD_REQUEST / MISSING_FIELD | 400 | Missing / malformed field |
| 401 | INVALID_JKEY / UNAUTHORIZED | 401 | jKey invalid or expired |
| 403 | FORBIDDEN | 403 | Insufficient privileges |
| 404 | RESOURCE_NOT_FOUND | 404 | Endpoint / symbol / order not found |
| 429 | RATE_LIMIT_EXCEEDED | 429 | Too many requests |
| 500 | INTERNAL_SERVER_ERROR | 500 | Server error |
| 503 | SERVICE_UNAVAILABLE | 503 | Service down / overloaded |

### Rate Limits

| Endpoint | Rate-Limit |
|---|---|
| Quote | 3 req/second (reduced to 1 req/sec) |
| Historical Candle | 3 req/second |
| Order Placement | 10 req/second |
| All other endpoints | 10 req/second |

---

## Data Types

### Exchanges

`BSE` (BSE Equity), `NSE` (NSE Equity), `NFO` (NSE F&O), `BFO` (BSE F&O)

### Products

`C` = Cash & Carry (delivery), `I` = Intraday (MIS), `M` = Market

### Price Types

`MKT` (Market), `LMT` (Limit), `SL-LMT` (Stop Loss Limit), `SL-MKT` (Stop Loss Market)

### Transaction Type

`B` (Buy), `S` (Sell)

---

## Authentication

### POST /login

`https://api.firstock.in/V1/login`

Body (all mandatory):

```json
{
  "userId": "AB1234",
  "password": "<SHA256(password)>",
  "TOTP": "123456",
  "vendorCode": "AB1234_API",
  "apiKey": "2b9d6c37476be0..."
}
```

`password` must be SHA256-hashed before sending. `TOTP` is required only if 2FA is
enabled. `vendorCode` and `apiKey` are issued by Firstock.

Success:

```json
{
  "status": "success",
  "message": "Login successful",
  "data": {
    "actid": "AB1234",
    "userName": "DEMO",
    "susertoken": "b6339fa5006155c2ae3611892cd80e0b8ae6cbe0dee0",
    "email": "example@gmail.com"
  }
}
```

Save `susertoken` — it is the `jKey` for all subsequent requests.

### POST /logout

Body: `{ "userId", "jKey" }`

Success:

```json
{ "status": "success", "message": "Successfully logged out" }
```

---

## User & Account

### POST /userDetails

Body: `{ "userId", "jKey" }`

Response `data`: `actid`, `email`, `exchange` (array of enabled exchanges), `orarr`
(supported order types), `requestTime`, `uprev` (privilege level), `userName`.

### POST /limit (RMS Limit)

Body: `{ "userId", "jKey" }`

Response `data`: `availableMargin`, `brkcollamt`, `cash`, `collateral`, `expo`,
`marginused`, `totalMargin`, `payin`, `peak_mar`, `premium`, `requestTime`, `span`.

---

## Orders

### POST /placeOrder

Body (all strings; mandatory unless noted):

```json
{
  "userId": "{{userId}}", "jKey": "{{jKey}}",
  "exchange": "NSE", "retention": "DAY", "product": "C",
  "priceType": "MKT", "tradingSymbol": "IDEA-EQ",
  "mkt_protection": "1", "transactionType": "B",
  "price": "0", "triggerPrice": "0", "quantity": "1", "remarks": "Test"
}
```

- `mkt_protection` is required and > 0 for `MKT` / `SL-MKT`; MKT orders are converted
  to LMT using best bid/ask (or LTP) adjusted by this percentage.
- Special symbols must be URL-encoded (e.g. `L&TFH-EQ` -> `L%26TFH-EQ`).
- Freeze-quantity slicing: oversized orders are split (max 10 slices).

Success:

```json
{
  "status": "success",
  "message": "Order details",
  "data": { "orderNumber": "25042100011119", "requestTime": "17:50:52 21-04-2025" }
}
```

### POST /modifyOrder

Body: `userId`, `jKey`, `orderNumber`, `product`, `priceType`, `tradingSymbol`, `price`,
`quantity`, `triggerPrice`, `retention`, `mkt_protection`, `exchange`.

For MKT / SL-MKT / SL-M orders, `mkt_protection` and `exchange` are mandatory.

### POST /cancelOrder

Body: `{ "userId", "jKey", "orderNumber" }`

Success:

```json
{
  "status": "success",
  "message": "Order cancellation details",
  "data": {
    "orderNumber": "25042100011119",
    "rejreason": "SAF:order is not open to cancel",
    "requestTime": "17:59:57 21-04-2025"
  }
}
```

### POST /orderBook

Body: `{ "userId", "jKey" }`

Response `data` (array of orders), each with: `averagePrice`, `exchange`, `fillShares`,
`lotSize`, `orderNumber`, `orderTime`, `price`, `priceType`, `product`, `quantity`,
`rejectReason`, `remarks`, `retention`, `status`, `tickSize`, `token`, `tradingSymbol`,
`transactionType`, `userId`.

### POST /singleOrderHistory

Body: `{ "userId", "jKey", "orderNumber" }`

Response `data` (array of lifecycle entries): `averagePrice`, `exchange`,
`exchangeOrderNum`, `exchangeTime`, `fillShares`, `orderNumber`, `orderTime`, `price`,
`priceType`, `product`, `quantity`, `rejectReason`, `remarks`, `reportType`, `retention`,
`status`, `tickSize`, `token`, `tradingSymbol`, `transactionType`, `userId`.

### POST /tradeBook

Body: `{ "userId", "jKey" }`

Response `data` (array of trades): `exchange`, `exchangeUpdateTime`, `exchordid`,
`fillId`, `fillPrice`, `fillQuantity`, `fillTime`, `fillshares`, `lotSize`,
`orderNumber`, `orderTime`, `priceFactor`, `pricePrecision`, `priceType`, `product`,
`quantity`, `retention`, `tickSize`, `token`, `tradingSymbol`, `transactionType`, `userId`.

### POST /orderMargin

Body:

```json
{
  "userId": "{{userId}}", "jKey": "{{jKey}}",
  "exchange": "NSE", "transactionType": "B", "product": "C",
  "tradingSymbol": "NIFTY", "quantity": "10", "priceType": "LMT", "price": "780"
}
```

Response `data`: `availableMargin`, `cash`, `marginOnNewOrder`, `remarks`, `requestTime`.

---

## After Market Orders (AMO)

### POST /placeAfterMarketOrder (curl path: /placeAMO)

Body: `userId`, `jKey`, `exchange`, `retention` (default DAY), `product`, `priceType`,
`tradingSymbol`, `mkt_protection`, `transactionType`, `price`, `triggerPrice`,
`quantity`, `remarks`. All numeric fields sent as strings.

### POST /modifyAfterMarketOrder (curl path: /modifyAMO)

Body: `userId`, `jKey`, `orderNumber`, `quantity`, `price`, `priceType`, `product`,
`mkt_protection`, `triggerPrice`.

### POST /basketOrder

Body:

```json
{
  "userId": "{{userId}}", "jKey": "{{jKey}}",
  "legs": [
    {
      "exchange": "NFO", "retention": "DAY", "product": "M", "priceType": "MKT",
      "tradingSymbol": "NIFTY28APR26C23700", "transactionType": "S", "price": "70.00",
      "triggerPrice": "0", "quantity": "65", "mkt_protection": "1", "remarks": "seq-leg1-buy-call"
    }
  ]
}
```

---

## Portfolio

### POST /positionBook

Body: `{ "userId", "jKey" }`

Response `data` (array), each with: `RealizedPNL`, `cfBuyAmt`, `cfBuyQty`, `cfSellAmt`,
`cfSellQty`, `dayBuyAmount`, `dayBuyAveragePrice`, `dayBuyQuantity`, `daySellAmount`,
`daySellAveragePrice`, `daySellQuantity`, `exchange`, `lastTradedPrice`, `lotSize`,
`netAveragePrice`, `netQuantity`, `netUploadPrice`, `product`, `tickSize`, `token`,
`totalMTM`, `totalPNL`, `tradingSymbol`, `uploadPrice`, `userId`.

### POST /holdings

Body: `{ "userId", "jKey" }` — returns `[{ "exchange", "tradingSymbol" }]`.

### POST /holdingsDetails

Body: `{ "userId", "jKey" }` — returns holdings with `exchangeTradingSymbol` array,
`sellAmount`, `holdQuantity`, `hairCut`, `dpQuantity`, `unPledgeQuantity`, `uploadPrice`,
`BTSTQuantity`, `usedQuantity`, `tradeQuantity`, `brokerCollateralQuantity`,
`beneficiaryQuantity`, `collateralQuantity`.

### POST /productConversion

Body:

```json
{
  "userId": "{{userId}}", "jKey": "{{jKey}}",
  "tradingSymbol": "IDEA-EQ", "exchange": "NSE",
  "previousProduct": "C", "product": "I", "quantity": "1", "msgFlag": "1"
}
```

`msgFlag`: `1` = Buy+Day, `2` = Buy+CF, `3` = Sell+Day, `4` = Sell+CF.

### POST /combinedHoldings

Body: `{ "userId", "jKey" }` — consolidated mutual funds + stocks + aggregated totals
(`mutualFunds`, `stocks`, `combinedHoldings`).

---

## Margin Utilities

### POST /basketMargin

Body: first order fields + `BasketList_Params` array of additional orders.

Response `data`: `BasketMargin`, `MarginOnNewOrder`, `PreviousMargin`, `TradedMargin`, `Remarks`.

### POST /brokerageCalculator

Body: `userId`, `jKey`, `exchange`, `tradingSymbol`, `transactionType`, `Product`,
`quantity`, `price`, `strike_price`, `inst_name`, `lot_size`.

---

## Market Data

### POST /securityInfo

Body: `{ "userId", "jKey", "exchange", "tradingSymbol" }`

Response `data`: `companyName`, `exchange`, `freezeQuantity`, `instrumentName`, `lotSize`,
`mult`, `priceFactor`, `pricePrecision`, `segment`, `symbolName`, `tickSize`, `token`,
`tradingSymbol`.

### POST /getQuote

Body: `{ "userId", "jKey", "exchange", "tradingSymbol" }`

Response `data` (array): full quote with `VWAP`, `bestBuyPrice1..5`, `bestBuyQuantity1..5`,
`bestSellPrice1..5`, `bestSellQuantity1..5`, `companyName`, `dayClosePrice`, `dayHighPrice`,
`dayLowPrice`, `dayOpenPrice`, `exchange`, `lastTradedPrice`, `lotSize`, `multipler`,
`openInterest`, `priceFactor`, `pricePrecision`, `segment`, `symbolName`, `tickSize`,
`token`, `totalBuyQuantity`, `totalSellQuantity`, `tradingSymbol`.

### POST /getQuote/ltp

Body: `{ "userId", "jKey", "exchange", "tradingSymbol" }`

Response `data` (array): `companyName`, `exchange`, `lastTradedPrice`, `requestTime`, `token`.

### POST /getMultiQuotes

Body: `{ "userId", "jKey", "data": [{ "exchange", "tradingSymbol" }, ...] }`

### POST /getMultiQuotes/ltp

Body: `{ "userId", "jKey", "data": [{ "exchange", "tradingSymbol" }, ...] }`

Response `data` (array): `companyName`, `exchange`, `identifier`, `lastTradedPrice`,
`requestTime`, `token`, `tradingSymbol`.

### POST /indexList

Body: `{ "userId", "jKey" }` — returns `[{ "exchange", "token", "tradingSymbol", "symbol", "idxname" }]`.

### POST /getExpiry

Body: `{ "userId", "jKey", "exchange", "tradingSymbol" }`

Response `data`: `{ "expiryDates": ["08MAY2025", "15MAY2025", ...] }` (DDMONYYYY format).

### POST /optionChain

Body: `{ "userId", "jKey", "exchange", "symbol", "expiry", "count", "strikePrice" }`

`count` = strikes above/below `strikePrice` (max 30).

Response `data` (array): `exchange`, `lotSize`, `optionType` (CE/PE), `parentToken`,
`pricePrecision`, `strikePrice`, `tickSize`, `token`, `tradingSymbol`, `lastTradedPrice`.

### POST /optionChainGreeks

Body: `userId`, `jKey`, `exchange`, `symbol`, `expiry`, `count` (max 30, or `ALL` for
full chain), `strikePrice` (not required when `count=ALL`).

Response `data` (array): option fields + Greeks `delta`, `gamma`, `theta`, `vega`,
`rho`, `iv`, `oi`.

### POST /searchScrips

Body: `{ "userId", "jKey", "stext" }`

Response `data` (array): `token`, `exchange`, `companyName`, `representationName`,
`instrumentName`, `tradingSymbol`.

---

## Historical Data (Time Price Series)

### POST /timePriceSeries

Body (regular interval):

```json
{
  "userId": "{{userId}}", "jKey": "{{jKey}}",
  "exchange": "NSE", "interval": "1mi", "tradingSymbol": "NIFTY",
  "startTime": "09:15:00 23-04-2025", "endTime": "15:29:00 23-04-2025"
}
```

Body (day interval): same but `interval: "1d"`.

Response `data` (array of candles): `time`, `epochTime`, `open`, `high`, `low`,
`close`, `volume`, `oi`.

---

## WebSocket V2

Connection: `wss://socket.firstock.in/V2/ws?userId={userId}&jKey={susertoken}&source=developer-api`

Ping-pong heartbeat: server pings; client must pong within 10s or the session is
terminated.

On connect, success = `{"status":"success","message":"Authentication successful"}`,
failure = `{"status":"failed","message":"unauthenticated"}` (then disconnected).

### Subscribe Feed

```json
{"action":"subscribe","tokens":"BSE:500470|NSE:26000"}
```

Market feed pushes per-token objects keyed by `EXCH:TOKEN` with `best_buy`, `best_sell`,
`c_exch_feed_time`, `c_exch_seg`, `i_last_traded_price`, `i_open_interest`, `i_volume_traded_today`, etc.

### Unsubscribe Feed

```json
{"action":"unsubscribe","tokens":"BSE:500470|NSE:26000"}
```

### Order Update Feed

Pushes order updates with fields: `tsym`, `rejreason`, `prcftr`, `pcode`, `trantype`,
`token`, `prctyp`, `ret`, `pp`, `ti`, `remarks`, `ntm`, `kidid`, `dscqty`,
`norenordno`, `mult`, `uid`, `exch`, `status`, `reporttype`, `tm`, `handlinst`, `ls`,
`actid`, `qty`, `prc`.

Order statuses (transition): `ORDER ACK`, `ORDER PENDING`, `OPEN`, `TRIGGER_PENDING`,
`AMO OPEN`, `AMO MODIFIED`, `AMO CANCELED`, `PENDING`. End states: `COMPLETE`,
`REJECTED`, `CANCELED`.

### Position Update Feed

Pushes position updates (identified by `brkname`) with fields: `child_orders`,
`upload_prc`, `totsellamt`, `daybuyqty`, `daybuyamt`, `sellavgprc`, `rpnl`,
`totbuyamt`, `daybuyavgprc`, `netqty`, `buyavgprc`, `totbuyavgprc`, `opensellqty`,
`openbuyamt`, `opensellamt`, `totsellavgprc`, `uptm`, `openbuyqty`, `brkname`,
`instname`.

---

## Error Handling Best Practices

- Always check `status` (`success` vs `failed`) before parsing `data`.
- On `failed`, inspect `error.field` and `error.message`.
- `code = "401"` / `INVALID_JKEY` means the session token expired — re-login.
- `code = "429"` means rate limit — throttle and retry with backoff.
- Batch requests where possible (e.g. `getMultiQuotes` / `getMultiQuotes/ltp`).
