# VM Deployment Guide — trader_hub (Firstock broker)

This repo is the trading VM for the **Firstock** broker. It is the
`Market_hub_corrected` (TradeX) VM with the broker layer swapped for Firstock
(see `ODIN_B2C_API_Reference.md` and `NSE.json` for instruments).

## Auth flow (2-step, TOTP)

1. `POST /api/auth/credentials` — stores Firstock credentials. Returns
   `requires_totp: true`.
2. `POST /api/auth/totp` with the TOTP — completes `POST /login`
   (`password` is SHA256-hashed, `TOTP` is an authenticator code the user owns;
   Firstock has no send-otp endpoint) and returns the session token (`susertoken`).

Credentials map:
| Plugin field | Firstock meaning |
|---|---|
| `user_id_broker` | Firstock `userId` |
| `api_key` | Firstock `apiKey` |
| `password` | Firstock password (hashed with SHA256 before sending) |
| `vendor_code` | Firstock `vendorCode` (e.g. `AB1234_API`) — required |
| `base_url` | Firstock API base URL (default `https://api.firstock.in/V1`) |
| `websocket_url` | Firstock WS URL (optional; default `wss://socket.firstock.in/V2/ws?...`) |

## VM Info
- Deploy path: `/home/admin/trader_hub_vm`
- Port: 8000

## Start / Stop / Restart

```bash
# Start
cd ~/trader_hub_vm
nohup python3 -m gunicorn -w 2 -k uvicorn.workers.UvicornWorker api_server:app --bind 0.0.0.0:8000 > error.log 2>&1 &

# Stop
pkill -f "gunicorn.*api_server"

# Restart
pkill -f "gunicorn.*api_server"
cd ~/trader_hub_vm
nohup python3 -m gunicorn -w 2 -k uvicorn.workers.UvicornWorker api_server:app --bind 0.0.0.0:8000 > error.log 2>&1 &
```

## Verify

```bash
ps aux | grep gunicorn | grep -v grep
curl -s http://localhost:8000/docs | head -5   # FastAPI Swagger
tail -f ~/trader_hub_vm/error.log
```

## Deploy new code from S3

```bash
aws s3 cp s3://<bucket>/trader_hub_vm_repo.tar.gz .
tar -xzf trader_hub_vm_repo.tar.gz
pkill -f "gunicorn.*api_server"
mv ~/trader_hub_vm ~/trader_hub_vm_old
mv ~/trader_hub_vm_repo ~/trader_hub_vm
pip install -r ~/trader_hub_vm/req.txt
cd ~/trader_hub_vm
nohup python3 -m gunicorn -w 2 -k uvicorn.workers.UvicornWorker api_server:app --bind 0.0.0.0:8000 > error.log 2>&1 &
sleep 2 && curl -s http://localhost:8000/docs | head -3
```

## Create a fresh tarball locally

```bash
cd /Users/anubhavkayal
tar --exclude='__pycache__' --exclude='.DS_Store' --exclude='.git' \
    --exclude='logs' --exclude='error.log' --exclude='*.csv' --exclude='*.env' \
    --no-xattrs -czf /tmp/trader_hub_vm_repo.tar.gz trader_hub_vm_repo/
aws s3 cp /tmp/trader_hub_vm_repo.tar.gz s3://<bucket>/
```

## Known limits (ponytail)
- **LTP**: Firstock has a real quotes endpoint; `live_ltp_ws_firstock.py` polls
  `getMultiQuotes/ltp` in ONE bulk call per cycle to stay within the ~1 req/sec
  quote rate limit. If real-time streaming is needed, subscribe to the V2
  WebSocket (`wss://socket.firstock.in/V2/ws?userId=..&jKey=..&source=developer-api`)
  — LTP ticks arrive there in paise.
- **Token expiry**: Firstock has no silent re-login; an expired `jKey`
  (`INVALID_JKEY`) requires the user to re-authenticate with a fresh TOTP.
