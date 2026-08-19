from datetime import datetime, timezone, timedelta
import os
import csv
from pymongo import MongoClient

from broker_factory import connect_broker, broker_config_from_session, BROKER_TRADEX, normalize_broker_type

MARKET_TZ = timezone(timedelta(hours=5, minutes=30))

DB_NAME = "autotrader"
COLLECTION_NAME = "order_history"

MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://ankitarrow:ankitarrow@cluster0.zcajdur.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
).strip()

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
orders_col = db[COLLECTION_NAME]


def _parse_order_time(raw_time):
    if not raw_time:
        return None
    if isinstance(raw_time, datetime):
        return raw_time.astimezone(MARKET_TZ).date()
    for fmt in ("%d-%b-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(str(raw_time)[:19], fmt).date()
        except ValueError:
            continue
    return None


def _normalize_intraday_order(o: dict) -> dict:
    product = (o.get("producttype") or o.get("product") or "").upper()
    side = o.get("transactiontype") or o.get("side") or ""
    sym = o.get("tradingsymbol") or o.get("symbol") or ""
    return {
        "order_id": o.get("orderid") or o.get("user_order_no"),
        "symbol": sym,
        "side": side,
        "qty": int(o.get("quantity") or o.get("qty") or 0),
        "filled_qty": int(o.get("filledshares") or o.get("qty_traded") or 0),
        "avg_price": float(o.get("averageprice") or o.get("average_fill_price") or 0),
        "status": o.get("orderstatus") or o.get("status"),
        "order_time": o.get("ordertime") or o.get("entry_at"),
        "update_time": o.get("updatetime") or o.get("last_modified"),
        "exchange_order_id": o.get("exchangeorderid") or o.get("exchange_order_no"),
        "rejection_reason": o.get("text") or o.get("reason") or "",
        "product": product,
    }


def fetch_todays_intraday_orders(broker=None, session=None, broker_config=None):
    """Fetch today's intraday orders via the active broker session."""
    today = datetime.now(MARKET_TZ).date()
    orders = []

    if broker is not None and session is not None:
        book = broker.get_order_book(session)
        if book.get("status") != "success":
            print("[WARN] order book failed:", book.get("error"))
            return []
        raw_orders = book.get("raw", {}).get("data", [])
    else:
        cfg = broker_config or {
            "broker_type": os.getenv("BROKER_TYPE", BROKER_TRADEX),
            "api_key": os.getenv("TRADEX_APP_KEY") or os.getenv("ANGEL_API_KEY"),
            "password": os.getenv("TRADEX_SECRET_KEY") or os.getenv("ANGEL_PASSWORD"),
            "client_code": os.getenv("TRADEX_CLIENT_ID") or os.getenv("ANGEL_CLIENT_CODE"),
            "user_id_broker": os.getenv("TRADEX_USER_ID"),
            "broker_session": {
                "token": os.getenv("TRADEX_TOKEN") or os.getenv("ANGEL_JWT_TOKEN"),
            },
        }
        if not cfg.get("api_key") or not cfg.get("client_code"):
            print("[WARN] broker credentials missing for orderbook fetch")
            return []
        broker, session = connect_broker(broker_config_from_session(cfg), restore=True)
        book = broker.get_order_book(session)
        if book.get("status") != "success":
            print("[WARN] order book failed:", book.get("error"))
            return []
        raw_orders = book.get("raw", {}).get("data", [])

    for o in raw_orders:
        try:
            row = _normalize_intraday_order(o)
            product = (row.get("product") or "").upper()
            if product not in ("INTRADAY", "MIS", "I"):
                continue
            order_date = _parse_order_time(row.get("update_time") or row.get("order_time"))
            if order_date != today:
                continue
            orders.append(row)
        except Exception as e:
            print("[PARSE ERROR]", e, o)

    return orders


def save_orders_to_csv(orders):
    if not orders:
        print("[WARN] No orders to save")
        return None

    today_str = datetime.now(MARKET_TZ).strftime("%Y-%m-%d")
    broker_tag = normalize_broker_type(os.getenv("BROKER_TYPE", BROKER_TRADEX))
    filename = f"{broker_tag}_orders_{today_str}.csv"

    filepath = os.path.join(os.getcwd(), filename)

    headers = [
        "order_id",
        "symbol",
        "side",
        "qty",
        "filled_qty",
        "avg_price",
        "status",
        "product",
        "order_time",
        "update_time",
        "exchange_order_id",
        "rejection_reason",
    ]

    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for o in orders:
            writer.writerow({k: o.get(k) for k in headers})

    print(f"[INFO] Saved {len(orders)} orders to {filepath}")
    return filepath
