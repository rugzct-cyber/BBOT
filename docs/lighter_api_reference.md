# Lighter DEX — API Reference for Bot Integration

> **Source**: [apidocs.lighter.xyz](https://apidocs.lighter.xyz/docs/get-started) + [WebSocket Reference](https://apidocs.lighter.xyz/docs/websocket-reference) + [Data Structures](https://apidocs.lighter.xyz/docs/data-structures-constants-and-errors)

---

## 1. Endpoints

| Protocol | URL | Purpose |
|---|---|---|
| **REST** | `https://mainnet.zklighter.elliot.ai` | Market data, account info, order submission |
| **WebSocket** | `wss://mainnet.zklighter.elliot.ai/stream` | Orderbook stream, account updates, **order placement** |
| Testnet REST | `https://testnet.zklighter.elliot.ai` | Testing |
| Testnet WS | `wss://testnet.zklighter.elliot.ai/stream` | Testing |

---

## 2. Authentication

### Account Structure
- **Ethereum wallet** → creates a Lighter account on-chain
- Each account gets an **account_index** (integer identifier)
- Sub-accounts possible (same L1 wallet, multiple indices)

### API Keys
- **Indices 0-1**: reserved (web/mobile UI)
- **Indices 2-254**: available for SDK/API (up to 253 keys)
- **Index 255**: special — queries data about all keys
- Each key has its own **nonce** (incremented per signed tx)

### SignerClient Initialization (Python SDK pattern)
```python
client = lighter.SignerClient(
    url=BASE_URL,
    api_private_keys={API_KEY_INDEX: PRIVATE_KEY},  # e.g. {2: "0xabc..."}
    account_index=ACCOUNT_INDEX                      # e.g. 42
)
```

### Auth Tokens (for authenticated REST/WS)
```python
auth_token, err = client.create_auth_token_with_expiry(
    deadline=3600,           # seconds, max 8 hours
    api_key_index=API_KEY_INDEX,
)
```

### Nonce Management
- Each nonce is **per API key** (not per account)
- SDK auto-manages nonces, or query `GET /api/v1/nextNonce?account_index=X&api_key_index=Y`

---

## 3. Market IDs & Precision

Query `GET /api/v1/orderBookDetails?filter=all` for all markets.

### Example Response (ETH)
```json
{
  "symbol": "ETH",
  "market_id": 0,
  "status": "active",
  "taker_fee": "0.0300",
  "maker_fee": "0.0000",
  "min_base_amount": "0.0050",
  "min_quote_amount": "10.000000",
  "supported_size_decimals": 4,
  "supported_price_decimals": 2,
  "size_decimals": 4,
  "price_decimals": 2,
  "initial_margin_fraction": 200,
  "maintenance_margin_fraction": 120,
  "closeout_margin_fraction": 80
}
```

### Known Market IDs
| Symbol | market_id | price_decimals | size_decimals |
|---|---|---|---|
| ETH | 0 | 2 | 4 |
| BTC | 1 (TBD - query) | 1 | 5 |
| SOL | TBD | 3 | 3 |

### Integer Encoding
**Prices and sizes are integers** in the API. The decimal precision determines the scale:
- ETH price `$3100.00` → `price = 310000` (2 decimals → ×100)
- ETH size `0.01` → `base_amount = 100` (4 decimals → ×10000)
- BTC price `$97500.1` → `price = 975001` (1 decimal → ×10)
- BTC size `0.00001` → `base_amount = 1` (5 decimals → ×100000)

---

## 4. Order Placement

### Order Types
```
ORDER_TYPE_LIMIT  = 0
ORDER_TYPE_MARKET = 1
```

### Time in Force
```
ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0  (IOC)
ORDER_TIME_IN_FORCE_GOOD_TILL_TIME      = 1  (GTT)
ORDER_TIME_IN_FORCE_POST_ONLY           = 2  (POST_ONLY)
```

### Transaction Types (for WS sendTx)
```
TxTypeL2CreateOrder        = 14
TxTypeL2CancelOrder        = 15
TxTypeL2CancelAllOrders    = 16
TxTypeL2ModifyOrder        = 17
TxTypeL2CreateGroupedOrders = 28
```

### Create Order (SDK fields)
```python
tx, tx_hash, err = await client.create_order(
    market_index=0,           # ETH perps
    client_order_index=1234,  # unique across all markets
    base_amount=10,           # in integer units (0.001 ETH for ETH market)
    price=3100_00,            # integer ($3100 for price_decimals=2)
    is_ask=False,             # False=Buy, True=Sell
    order_type=client.ORDER_TYPE_MARKET,
    time_in_force=client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
    reduce_only=False,
    order_expiry=client.DEFAULT_IOC_EXPIRY,  # ms timestamp
)
```

### Cancel Order
```python
tx, tx_hash, err = await client.cancel_order(
    market_index=market_index,
    order_index=1234   # the order_index from active orders
)
```

### Modify Order
```python
tx, tx_hash, err = await client.modify_order(
    market_index=0,
    order_index=1234,
    base_amount=100,
    price=2800_00,
)
```

---

## 5. WebSocket Channels

### 5.1 Order Book Stream

**Subscribe:**
```json
{"type": "subscribe", "channel": "order_book/{MARKET_INDEX}"}
```
Example: `{"type": "subscribe", "channel": "order_book/0"}` (ETH)

**Update format (every 50ms):**
```json
{
  "channel": "order_book:0",
  "offset": 41692864,
  "order_book": {
    "code": 0,
    "asks": [{"price": "3327.46", "size": "29.0915"}],
    "bids": [{"price": "3338.80", "size": "10.2898"}],
    "offset": 41692864,
    "nonce": 4037957053,
    "begin_nonce": 4037957034
  },
  "timestamp": 1766434222583,
  "type": "update/order_book"
}
```

**Behavior:**
- Sends **full snapshot** on first subscribe
- Then **delta updates only** (every ~50ms)
- Verify continuity: `begin_nonce` of current update == `nonce` of previous update
- On reconnect to a different server, `offset` may jump

### 5.2 Send Transaction via WS

**Single order:**
```json
{
  "type": "jsonapi/sendtx",
  "data": {
    "tx_type": 14,    // TxTypeL2CreateOrder
    "tx_info": {...}  // signed tx payload from SignerClient
  }
}
```

**Batch (up to 50 orders):**
```json
{
  "type": "jsonapi/sendtxbatch",
  "data": {
    "tx_types": "[14, 14]",
    "tx_infos": "[{...}, {...}]"
  }
}
```

### 5.3 Account All (positions + trades + assets)

**Subscribe:**
```json
{"type": "subscribe", "channel": "account_all/{ACCOUNT_INDEX}"}
```

**Response includes:**
- `positions` → Map of market_index → Position
- `trades` → Map of market_index → [Trade]
- `assets` → [Asset] (balances)
- `funding_histories` → funding payments

### 5.4 Trade Stream

**Subscribe:**
```json
{"type": "subscribe", "channel": "trade/{MARKET_INDEX}"}
```

### 5.5 Account Orders (requires auth)

**Subscribe:**
```json
{"type": "subscribe", "channel": "account_orders/{ACCOUNT_INDEX}/{MARKET_INDEX}"}
```

---

## 6. Data Structures

### Position JSON
```json
{
  "market_id": 1,
  "symbol": "BTC-USD",
  "sign": 1,                              // 1=long, -1=short, 0=no position
  "position": "0.5",                       // absolute size
  "avg_entry_price": "97500.00",
  "position_value": "48750.00",
  "unrealized_pnl": "500.00",
  "realized_pnl": "100.00",
  "liquidation_price": "82000.00",
  "initial_margin_fraction": "0.1",
  "margin_mode": 1,
  "allocated_margin": "4875"
}
```

### Order JSON
```json
{
  "order_index": 281474992718570,
  "client_order_index": 1234,
  "market_index": 0,
  "initial_base_amount": "1.2001",
  "remaining_base_amount": "0.4810",
  "price": "2310.53",
  "is_ask": true,
  "filled_base_amount": "0.7191",
  "filled_quote_amount": "1662.54",
  "side": "sell",
  "type": "limit",
  "time_in_force": "good_till_time",
  "status": "active",
  "order_expiry": 1728833297023,
  "timestamp": 1728722474677
}
```

### Trade JSON
```json
{
  "trade_id": 14035051,
  "tx_hash": "189068ebc6b5c7e5...",
  "type": "trade",
  "market_id": 0,
  "size": "0.1187",
  "price": "3335.65",
  "usd_amount": "13.67",
  "is_maker_ask": false,
  "taker_fee": 0,            // omitted when zero
  "maker_fee": 0,
  "timestamp": 1722339648
}
```

### Order Status Codes
```
0 = InProgress       (being registered)
1 = Pending          (pending trigger)
2 = Active           (limit order in book)
3 = Filled
4 = Canceled
5 = Canceled_PostOnly
6 = Canceled_ReduceOnly
7 = Canceled_PositionNotAllowed
8 = Canceled_MarginNotAllowed
9 = Canceled_TooMuchSlippage
10 = Canceled_NotEnoughLiquidity
11 = Canceled_SelfTrade
12 = Canceled_Expired
```

### Transaction Status
```
0 = Failed
1 = Pending
2 = Executed
3 = Pending (Final State)
```

---

## 7. REST Endpoints (Key Ones)

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/v1/status` | Exchange status |
| GET | `/api/v1/orderBookDetails` | Market configs (precision, fees, margins) |
| GET | `/api/v1/orderBookOrders?market_id=X&limit=N` | Current orderbook snapshot |
| GET | `/api/v1/account?index=X` | Account info (positions, balances) |
| GET | `/api/v1/accountsByL1Address?l1_address=0x...` | Find account index from wallet |
| GET | `/api/v1/accountActiveOrders?account_index=X&market_id=Y` | Active orders |
| GET | `/api/v1/recentTrades?market_id=X` | Recent trade history |
| GET | `/api/v1/nextNonce?account_index=X&api_key_index=Y` | Next nonce for signing |
| GET | `/api/v1/pnl?account_index=X` | PnL data |
| GET | `/api/v1/funding-rates` | Current funding rates |
| POST | `/api/v1/sendTx` | Submit signed transaction |
| POST | `/api/v1/sendTxBatch` | Submit batch of signed transactions |

---

## 8. Signing Mechanism (for Rust impl)

The Go SDK source is the reference for our Rust implementation:
- **Repo**: [github.com/elliottech/lighter-go](https://github.com/elliottech/lighter-go)
- **Signer code**: `lighter-go/signer_client.go`
- **Constants**: `lighter-go/types/txtypes/constants.go`

### Signing Flow
1. Build transaction payload (e.g., CreateOrder struct)
2. Serialize to signing payload (deterministic format)
3. Sign with Ethereum private key (ECDSA secp256k1)
4. Submit via REST `sendTx` or WS `jsonapi/sendtx`

### For our Rust adapter:
- Use `ethers` or `alloy` crate for ECDSA signing
- Study Go SDK's `sign_create_order` for exact payload format
- Nonce auto-increment per API key

---

## 9. Key Differences vs Vest/Paradex

| Feature | Lighter | Vest | Paradex |
|---|---|---|---|
| **Currency** | USDC | USDC | USD (needs Pyth conversion) |
| **Fees** | 0/0 (Standard), tiered (Premium) | Maker/Taker fees | Maker/Taker fees |
| **Order via** | WS `sendtx` or REST `sendTx` | REST POST | REST POST |
| **Orderbook WS** | `order_book/{market_id}` (50ms) | `@depth` stream | `book` channel |
| **Auth** | ETH private key + API key index + nonce | API key/secret | ETH private key + StarkNet |
| **Price format** | Integer (scaled by decimals) | Decimal string | Decimal string |
| **Size format** | Integer (scaled by decimals) | Decimal string | Decimal string |
| **Position via** | WS `account_all/{id}` or REST | REST `/positions` | REST `/positions` |
| **Self-trade** | Cancels resting (maker) order | N/A | N/A |

---

## 10. Implementation Notes for Rust Adapter

### Priority for HFT
1. **WS for orderbook** — subscribe `order_book/{market_id}`, parse snapshot + deltas
2. **WS for orders** — use `jsonapi/sendtx` for lowest latency (vs REST POST)
3. **WS for account** — subscribe `account_all/{account_index}` for position tracking
4. **REST for initialization** — query `orderBookDetails` once at startup for market configs

### Integer Price/Size Conversion
```rust
// Convert float price to Lighter integer
fn to_lighter_price(price: f64, price_decimals: u32) -> u64 {
    let multiplier = 10u64.pow(price_decimals);
    (price * multiplier as f64).round() as u64
}

// Convert float size to Lighter integer
fn to_lighter_size(size: f64, size_decimals: u32) -> u64 {
    let multiplier = 10u64.pow(size_decimals);
    (size * multiplier as f64).round() as u64
}

// Convert Lighter integer back to float
fn from_lighter_price(price_int: u64, price_decimals: u32) -> f64 {
    price_int as f64 / 10u64.pow(price_decimals) as f64
}
```

### For IOC Market Order (our primary order type)
```
order_type = 1          (MARKET)
time_in_force = 0       (IOC)
is_ask = true/false     (sell/buy)
price = worst_acceptable_price (slippage limit)
```
