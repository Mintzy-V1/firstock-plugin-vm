"""
Firstock broker adapter — mirrors broker_tradex.BrokerConnector surface so the
shared engine (api_server, auto_trader, session_manager, orderbook) works
unchanged against the Firstock REST API (https://api.firstock.in/V1, V2 WS).

Login is single-step: POST /login with userId + SHA256(password) + TOTP +
vendorCode + apiKey -> data.susertoken (the jKey). Every other call carries
{userId, jKey}. There is no send-otp endpoint — TOTP is a static authenticator
code the user already has.
"""

import hashlib
import json
import os
import time

import requests

from broker_factory import BROKER_FIRSTOCK


class FirstockAPIError(Exception):
    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


class FirstockAuthError(FirstockAPIError):
    pass


EXCHANGE_MAP = {
    "NSE": "NSE", "NSE_EQ": "NSE",
    "BSE": "BSE", "BSE_EQ": "BSE",
    "NFO": "NFO", "NSE_FO": "NFO",
    "BFO": "BFO", "BSE_FO": "BFO",
    "MCX": "MCX", "MCX_FO": "MCX",
    "NCDEX": "NCDEX", "NCDEX_FO": "NCDEX",
}
ORDER_TYPE_MAP = {"MARKET": "MKT", "LIMIT": "LMT", "SL": "SL-LMT", "SL-MKT": "SL-MKT"}
PRODUCT_TYPE_MAP = {"INTRADAY": "I", "CNC": "C", "DELIVERY": "C", "BTST": "C", "MTF": "I"}
TERMINAL_STATUSES = {
    "REJECTED": "rejected", "CANCELED": "cancelled", "CANCELLED": "cancelled",
    "COMPLETE": "filled", "FILLED": "filled", "EXECUTED": "filled",
    "TRIGGERED": "open",
}


class _FirstockClient:
    """Thin REST wrapper for the Firstock V1 endpoints used by this VM."""

    def __init__(self, base_url: str, api_key: str, user_id: str, vendor_code: str):
        if not base_url:
            raise RuntimeError("FIRSTOCK_BASE_URL not set (pass base_url in credentials)")
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.user_id = user_id
        self.vendor_code = vendor_code
        self.jkey = None
        self.http = requests.Session()

    def _request(self, path: str, payload: dict) -> dict:
        url = f"{self.base_url}{path}"
        try:
            resp = self.http.post(url, json=payload, timeout=15)
        except requests.RequestException as e:
            raise FirstockAPIError(f"Firstock request failed: {e}") from e
        try:
            body = resp.json()
        except ValueError:
            body = {"status": "failed", "error": {"message": resp.text[:300]}}

        code = body.get("code")
        if resp.status_code == 401 or code == 401 or "INVALID_JKEY" in str(body.get("name", "")).upper():
            raise FirstockAuthError(f"Firstock auth failed: {body.get('name')} - {body.get('error')}")

        if (body.get("status") or "").lower() != "success":
            err = (body.get("error") or {})
            msg = err.get("message") or err.get("field") or body.get("name") or str(body)[:300]
            raise FirstockAPIError(f"Firstock {path} failed: {msg}", resp.status_code)
        return body

    def _auth_payload(self) -> dict:
        return {"userId": self.user_id, "jKey": self.jkey or ""}

    # ---- auth ----
    def login(self, password: str, totp: str) -> dict:
        payload = {
            "userId": self.user_id,
            "password": hashlib.sha256(password.encode()).hexdigest(),
            "TOTP": totp,
            "vendorCode": self.vendor_code,
            "apiKey": self.api_key,
        }
        resp = self._request("/login", payload)
        data = resp.get("data") or {}
        self.jkey = data.get("susertoken") or data.get("jKey")
        return resp

    def logout(self) -> dict:
        return self._request("/logout", self._auth_payload())

    def user_details(self) -> dict:
        return self._request("/userDetails", self._auth_payload())

    # ---- orders ----
    def place_order(self, payload: dict) -> dict:
        body = self._auth_payload()
        body.update(payload)
        return self._request("/placeOrder", body)

    def modify_order(self, payload: dict) -> dict:
        body = self._auth_payload()
        body.update(payload)
        return self._request("/modifyOrder", body)

    def cancel_order(self, order_number: str) -> dict:
        body = self._auth_payload()
        body["orderNumber"] = order_number
        return self._request("/cancelOrder", body)

    def get_order_book(self) -> dict:
        return self._request("/orderBook", self._auth_payload())

    def single_order_history(self, order_number: str) -> dict:
        body = self._auth_payload()
        body["orderNumber"] = order_number
        return self._request("/singleOrderHistory", body)

    def get_trade_book(self) -> dict:
        return self._request("/tradeBook", self._auth_payload())

    # ---- portfolio / funds ----
    def get_positions(self) -> dict:
        return self._request("/positionBook", self._auth_payload())

    def get_holdings(self) -> dict:
        return self._request("/holdingsDetails", self._auth_payload())

    def get_balance(self) -> dict:
        return self._request("/limit", self._auth_payload())

    # ---- market data ----
    def get_quote_ltp(self, quotes: list) -> dict:
        body = self._auth_payload()
        body["data"] = quotes
        return self._request("/getQuote/ltp", body)

    def get_multi_quotes_ltp(self, quotes: list) -> dict:
        body = self._auth_payload()
        body["data"] = quotes
        return self._request("/getMultiQuotes/ltp", body)

    def security_info(self, exchange: str, trading_symbol: str) -> dict:
        body = self._auth_payload()
        body["exchange"] = exchange
        body["tradingSymbol"] = trading_symbol
        return self._request("/securityInfo", body)


class BrokerConnector:
    """Firstock REST adapter with the same public surface as broker_tradex.BrokerConnector."""

    broker_type = BROKER_FIRSTOCK

    def __init__(self, instruments_path="NSE.json", require_totp=True):
        self.user_id = os.getenv("FIRSTOCK_USER_ID", "").strip()
        self.api_key = os.getenv("FIRSTOCK_API_KEY", "").strip()
        self.password = os.getenv("FIRSTOCK_PASSWORD", "").strip()
        self.base_url = os.getenv("FIRSTOCK_BASE_URL", "").strip()
        self.vendor_code = os.getenv("FIRSTOCK_VENDOR_CODE", "").strip()
        self.totp = os.getenv("FIRSTOCK_TOTP", "").strip()
        self.instruments_path = instruments_path
        self.require_totp = require_totp

        self.obj = None
        self.access_token = None
        self.user = None
        self._load_instruments()

    def _load_instruments(self):
        try:
            with open(self.instruments_path, "r") as f:
                self._instruments = json.load(f)
        except Exception:
            self._instruments = []

    def _build_client(self) -> _FirstockClient:
        if not self.user_id or not self.api_key or not self.password or not self.vendor_code:
            raise RuntimeError("Firstock credentials missing (user_id, api_key, password, vendor_code)")
        return _FirstockClient(self.base_url, self.api_key, self.user_id, self.vendor_code)

    def _session_dict(self) -> dict:
        return {
            "user": self.user or self.user_id,
            "token": self.access_token,
            "refresh_token": None,
            "feed_token": None,
            "obj": self.obj,
        }

    def send_otp(self):
        # Firstock has no send-otp endpoint; TOTP is an authenticator code the user owns.
        print(f"[FIRSTOCK] TOTP must be entered from your authenticator app for user {self.user_id}")

    def _create_session(self):
        if self.require_totp and not self.totp:
            raise RuntimeError("Firstock login requires the TOTP (authenticator code). Pass totp.")
        self.obj = self._build_client()
        resp = self.obj.login(self.password, self.totp)
        if not self.obj.jkey:
            raise RuntimeError(f"Firstock login failed: {resp}")
        self.access_token = self.obj.jkey
        self.user = self.user_id
        print("Firstock account linked successfully.")
        return self._session_dict()

    def restore_session(self, broker_session: dict):
        jkey = (broker_session or {}).get("token") or ""
        if jkey.startswith("Bearer "):
            jkey = jkey.replace("Bearer ", "", 1)
        self.obj = self._build_client()
        self.obj.jkey = jkey
        self.access_token = jkey
        self.user = self.user_id
        # ponytail: no silent re-login — an expired jKey surfaces as INVALID_JKEY
        # on the next call and triggers SESSION_EXPIRED_RELOGIN_REQUIRED upstream.
        return self._session_dict()

    def get_session(self):
        if self.obj and self.access_token:
            return self._session_dict()
        return self._create_session()

    def refresh_session(self):
        return self._create_session()

    def get_ws_credentials(self):
        url = f"wss://socket.firstock.in/V2/ws?userId={self.user_id}&jKey={self.access_token or ''}&source=developer-api"
        return {
            "auth_token": self.access_token or "",
            "feed_token": self.access_token or "",
            "api_key": self.api_key,
            "client_code": self.user_id,
            "websocket_url": url,
        }

    def _is_auth_error(self, err):
        if isinstance(err, FirstockAuthError):
            return True
        text = str(err).lower()
        return any(m in text for m in ("401", "invalid_jkey", "unauthorized", "token expired"))

    def _call_api(self, fn, *args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return fn(*args, **kwargs)
            except FirstockAuthError:
                raise RuntimeError("SESSION_EXPIRED_RELOGIN_REQUIRED")
            except FirstockAPIError as e:
                if e.status_code == 429 and attempt < max_retries - 1:
                    wait = 2 ** attempt
                    print(f"[RATE LIMIT] 429 on attempt {attempt+1}/{max_retries}, retrying in {wait}s")
                    time.sleep(wait)
                    continue
                raise

    # ---- symbol / instrument lookup ----
    def get_symbol_token(self, tradingsymbol):
        sym = tradingsymbol.upper().replace("-EQ", "")
        for item in self._instruments:
            if not isinstance(item, dict):
                continue
            raw = (item.get("symbol") or item.get("tradingsymbol") or "").upper().replace("-EQ", "")
            if raw == sym:
                token = item.get("exchange_token") or item.get("token") or item.get("code")
                if token:
                    return str(token)
        return None

    @staticmethod
    def _map_exchange(exchange: str) -> str:
        return EXCHANGE_MAP.get((exchange or "").upper(), exchange or "NSE")

    @staticmethod
    def _map_order_type(order_type: str) -> str:
        return ORDER_TYPE_MAP.get((order_type or "").upper(), "MKT")

    @staticmethod
    def _map_product_type(product_type: str) -> str:
        return PRODUCT_TYPE_MAP.get((product_type or "").upper(), product_type or "M")

    @staticmethod
    def _ensure_eq(symbol: str) -> str:
        sym = (symbol or "").upper()
        return sym if sym.endswith("-EQ") else f"{sym}-EQ"

    # ---- normalization (Firstock rows -> Angel-like dicts for the engine) ----
    def _normalize_position_row(self, pos) -> dict:
        symbol = self._ensure_eq(pos.get("tradingSymbol") or pos.get("tradingsymbol") or pos.get("symbol") or "")
        net_qty = int(pos.get("netQuantity") or pos.get("netqty") or 0)
        avg_price = float(pos.get("netAveragePrice") or pos.get("netavgprice") or 0)
        ltp = float(pos.get("lastTradedPrice") or pos.get("ltp") or 0)
        return {
            "tradingsymbol": symbol,
            "netqty": net_qty,
            "averageprice": avg_price,
            "avgprice": avg_price,
            "ltp": ltp,
            "lastprice": ltp,
            "producttype": pos.get("product", ""),
            "exchange": pos.get("exchange", ""),
            "code": str(pos.get("token") or ""),
        }

    def _normalize_order_row(self, order) -> dict:
        status = (order.get("status") or "").upper()
        if status in ("FILLED", "COMPLETE", "EXECUTED"):
            orderstatus = "complete"
        elif status in ("CANCELED", "CANCELLED"):
            orderstatus = "cancelled"
        elif status in ("REJECTED", "PENDING", "OPEN"):
            orderstatus = "rejected" if status == "REJECTED" else status.lower()
        else:
            orderstatus = status.lower() or "open"
        symbol = self._ensure_eq(order.get("tradingSymbol") or order.get("tradingsymbol") or "")
        price = float(order.get("price") or 0)
        side = order.get("transactionType") or order.get("transactiontype")
        return {
            "orderid": order.get("orderNumber") or order.get("ordernumber"),
            "tradingsymbol": symbol,
            "orderstatus": orderstatus,
            "averageprice": price,
            "filledshares": int(order.get("filledShares") or 0),
            "price": price,
            "producttype": (order.get("product") or "").upper(),
            "transactiontype": "BUY" if side in ("B", "BUY") else "SELL" if side in ("S", "SELL") else side,
            "text": order.get("rejectReason") or order.get("statusMessage") or order.get("message") or "",
            "code": str(order.get("token") or ""),
            "exchange": order.get("exchange"),
            "exchange_order_no": order.get("orderNumber"),
        }

    def _normalize_holding_row(self, holding) -> dict:
        secs = holding.get("exchangeTradingSymbol") or holding.get("exchangeTradingSymbols") or []
        symbol = ""
        for sec in secs:
            if (sec.get("exchange") or "").upper() in ("NSE", "BSE"):
                symbol = sec.get("tradingSymbol", "")
                break
        if not symbol and secs:
            symbol = (secs[0] or {}).get("tradingSymbol", "")
        return {
            "symbol": symbol,
            "tradingsymbol": symbol,
            "quantity": int(holding.get("holdQuantity") or holding.get("quantity") or 0),
            "average_price": float(holding.get("uploadPrice") or holding.get("averagePrice") or 0),
            "last_price": 0.0,
            "pnl": 0.0,
            "product": holding.get("product", ""),
            "isin": holding.get("isin", ""),
        }

    # ---- account ----
    def get_account_balance(self, session):
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            balance = self._call_api(session["obj"].get_balance)
            data = balance.get("data") or {}
            free_cash = float(data.get("cash") or data.get("availableMargin") or 0)
            margin_used = float(data.get("marginused") or data.get("marginUsed") or 0)

            holdings_resp = self._call_api(session["obj"].get_holdings)
            positions_resp = self._call_api(session["obj"].get_positions)

            holdings = [self._normalize_holding_row(h) for h in (holdings_resp.get("data") or [])]
            positions = [self._normalize_position_row(p) for p in (positions_resp.get("data") or [])]

            print("\n========== ACCOUNT SUMMARY (FIRSTOCK) ==========")
            print(f"Available Cash: {free_cash:,.2f}")
            print(f"Margin Used: {margin_used:,.2f}")
            print("==============================================\n")

            return {
                "status": "success",
                "free_cash": free_cash,
                "collateral": float(data.get("collateral") or 0),
                "margin_used": margin_used,
                "holdings": holdings,
                "positions": positions,
                "raw": balance,
                "source": "FIRSTOCK",
            }
        except RuntimeError:
            raise
        except Exception as ex:
            return {"status": "error", "error": str(ex), "source": "FIRSTOCK"}

    # ---- orders ----
    def _wait_for_order_confirmation(self, session, order_id, max_wait_seconds=10, check_interval=1):
        if not order_id:
            return {"filled": False, "status": "NO_ORDER_ID", "message": "No order ID provided"}
        elapsed = 0
        while elapsed < max_wait_seconds:
            try:
                book = self._call_api(session["obj"].get_order_book)
                for order in book.get("data") or []:
                    if str(order.get("orderNumber")) != str(order_id):
                        continue
                    status = (order.get("status") or "").upper()
                    if status in ("FILLED", "COMPLETE", "EXECUTED"):
                        return {"filled": True, "status": status, "avg_price": float(order.get("price") or 0),
                                "message": f"Order {order_id} filled"}
                    if status in ("REJECTED", "CANCELED", "CANCELLED"):
                        return {"filled": False, "status": status, "avg_price": 0,
                                "message": order.get("rejectReason") or f"Order {order_id} {status.lower()}"}
                time.sleep(check_interval)
                elapsed += check_interval
            except RuntimeError:
                raise
            except Exception as e:
                print(f"[WARN] Firstock order status check failed: {e}")
                time.sleep(check_interval)
                elapsed += check_interval
        return {"filled": False, "status": "TIMEOUT", "avg_price": 0,
                "message": f"Order {order_id} status check timed out after {max_wait_seconds}s"}

    def place_order(
        self,
        session,
        symbol,
        side,
        qty=None,
        quantity=None,
        price=None,
        order_type="MARKET",
        product_type="INTRADAY",
        exchange="NSE",
        variety="NORMAL",
        lot_based=False,
        stop_loss=None,
        trigger_price=None,
        wait_for_confirmation=True,
        scrip_consent="YES",
        **kwargs,
    ):
        qty = qty or quantity
        if qty is None:
            return {"status": "error", "error": "Missing quantity/qty argument", "filled": False}
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided.", "filled": False}

        side_upper = side.upper().replace("_", "").replace(" ", "")
        if side_upper in ("BUY", "LONG", "BUYCOVER", "COVER"):
            transaction_type = "B"
        elif side_upper in ("SELL", "SHORT", "SHORTSELL", "SELLSHORT"):
            transaction_type = "S"
        else:
            return {"status": "error", "error": f"Invalid side: {side}", "filled": False}

        is_market = order_type.upper() == "MARKET"
        price_type = self._map_order_type(order_type)
        if (stop_loss or trigger_price) and order_type.upper() in ("MARKET", "LIMIT"):
            price_type = "SL-MKT" if is_market else "SL-LMT"
        trigger = float(trigger_price or stop_loss or 0)

        payload = {
            "exchange": self._map_exchange(exchange),
            "retention": "DAY",
            "product": self._map_product_type(product_type),
            "priceType": price_type,
            "tradingSymbol": self._ensure_eq(symbol),
            "mkt_protection": "1" if price_type in ("MKT", "SL-MKT") else "0",
            "transactionType": transaction_type,
            "price": "0" if is_market else str(float(price or 0)),
            "triggerPrice": str(trigger),
            "quantity": str(int(qty)),
            "remarks": "",
        }

        try:
            print(f"[ORDER] FIRSTOCK {transaction_type} {payload['tradingSymbol']} x {qty} "
                  f"product={payload['product']} priceType={price_type}")
            resp = self._call_api(session["obj"].place_order, payload)
            order_id = str(((resp.get("data") or {}).get("orderNumber") or "")).strip() or None

            if not order_id:
                return {"status": "error", "error": "No order ID in response", "filled": False, "raw": resp}

            if wait_for_confirmation:
                confirmation = self._wait_for_order_confirmation(session, order_id)
                if confirmation.get("filled"):
                    return {"status": "success", "order_id": order_id, "filled": True,
                            "avg_price": confirmation["avg_price"], "raw": resp}
                return {"status": "error", "order_id": order_id, "filled": False,
                        "error": confirmation["message"], "raw": resp}
            return {"status": "success", "order_id": order_id, "filled": None, "raw": resp}
        except RuntimeError:
            raise
        except Exception as e:
            return {"status": "error", "error": str(e), "filled": False}

    def _find_order_row(self, session, order_id):
        book = self.get_order_book(session)
        if book.get("status") != "success":
            return None
        for order in book.get("raw", {}).get("data", []):
            if str(order.get("orderid")) == str(order_id):
                return order
        return None

    def modify_order(self, session, order_id, new_price=None, new_qty=None, **kwargs):
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            book = self._call_api(session["obj"].get_order_book)
            current = next((o for o in (book.get("data") or []) if str(o.get("orderNumber")) == str(order_id)), None)
            if not current:
                return {"status": "error", "error": f"Order {order_id} not found in order book"}

            price_type = (current.get("priceType") or "MKT").upper()
            payload = {
                "orderNumber": order_id,
                "exchange": current.get("exchange") or "NSE",
                "retention": current.get("retention") or "DAY",
                "product": current.get("product") or "I",
                "priceType": price_type,
                "tradingSymbol": current.get("tradingSymbol") or self._ensure_eq(kwargs.get("symbol") or ""),
                "mkt_protection": "1" if price_type in ("MKT", "SL-MKT") else "0",
                "price": str(new_price if new_price is not None else float(current.get("price") or 0)),
                "quantity": str(int(new_qty or current.get("quantity") or 0)),
                "triggerPrice": str(kwargs.get("trigger_price") or current.get("triggerPrice") or 0),
            }
            resp = self._call_api(session["obj"].modify_order, payload)
            return {"status": "success", "raw": resp}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def cancel_order(self, session, order_id, variety="NORMAL"):
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            resp = self._call_api(session["obj"].cancel_order, order_id)
            return {"status": "success", "raw": resp}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_order_book(self, session):
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            resp = self._call_api(session["obj"].get_order_book)
            data = [self._normalize_order_row(o) for o in (resp.get("data") or [])]
            return {"status": "success", "raw": {"status": True, "data": data}}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_order_status(self, session, order_id):
        try:
            book = self.get_order_book(session)
            if book.get("status") != "success":
                return {"status": "error", "error": book.get("error", "order book failed")}
            for order in book.get("raw", {}).get("data", []):
                if str(order.get("orderid")) == str(order_id):
                    return order
            return {"status": "error", "error": f"Order {order_id} not found"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # ---- portfolio ----
    def get_positions(self, session):
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            resp = self._call_api(session["obj"].get_positions)
            data = [self._normalize_position_row(p) for p in (resp.get("data") or [])]
            return {"status": "success", "raw": {"status": True, "data": data}}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_holdings(self, session):
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            resp = self._call_api(session["obj"].get_holdings)
            data = [self._normalize_holding_row(h) for h in (resp.get("data") or [])]
            return {"status": "success", "raw": {"status": True, "data": data}}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_trade_book(self, session):
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            resp = self._call_api(session["obj"].get_trade_book)
            data = resp.get("data") or []
            return {"status": "success", "raw": {"status": True, "data": data}}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # ---- LTP (real quotes: getQuote/ltp, getMultiQuotes/ltp) ----
    # ponytail: Firstock quote endpoints key on tradingSymbol (not token);
    # _quote_items builds {exchange, tradingSymbol} and maps responses back
    # by the same tradingSymbol so the engine sees its plain symbol names.
    def _quote_items(self, symbols, exchange="NSE"):
        items, by_sym = [], {}
        for s in symbols:
            tsym = self._ensure_eq(s)
            items.append({"exchange": self._map_exchange(exchange), "tradingSymbol": tsym})
            by_sym[tsym] = s.upper()
        return items, by_sym

    def get_ltp(self, session, exchange, trading_symbol, symbol_token):
        if not session or "obj" not in session:
            return {"status": "error", "error": "No active session object provided."}
        try:
            tsym = self._ensure_eq(trading_symbol)
            resp = self._call_api(session["obj"].get_quote_ltp, [{"exchange": self._map_exchange(exchange), "tradingSymbol": tsym}])
            ltp = 0.0
            for row in resp.get("data") or []:
                if str(row.get("tradingSymbol") or "").upper() == tsym:
                    ltp = float(row.get("lastTradedPrice") or row.get("last_traded_price") or 0)
                    break
            return {"status": "success", "raw": {"status": True, "data": {"ltp": ltp}}}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_bulk_ltp(self, session, symbols):
        return self._get_bulk_broker_ltp(symbols, session=session)

    def _get_bulk_broker_ltp(self, symbols, session=None, exchange="NSE"):
        session = session or self.get_session()
        syms = [s.upper() for s in symbols if s]
        if not syms:
            return {}
        items, by_sym = self._quote_items(syms, exchange=exchange)
        if not items:
            return {}
        try:
            resp = self._call_api(session["obj"].get_multi_quotes_ltp, items)
        except RuntimeError:
            raise
        except Exception as e:
            print(f"[LTP ERROR] {e}")
            return {}
        out = {}
        for row in resp.get("data") or []:
            price = float(row.get("lastTradedPrice") or row.get("last_traded_price") or 0)
            if price <= 0:
                continue
            sym = by_sym.get(str(row.get("tradingSymbol") or "").upper())
            if sym and sym not in out:
                out[sym] = price
        return out


if __name__ == "__main__":
    import os
    os.environ["FIRSTOCK_USER_ID"] = "TEST1"
    os.environ["FIRSTOCK_API_KEY"] = "testkey"
    os.environ["FIRSTOCK_PASSWORD"] = "testpass"
    os.environ["FIRSTOCK_BASE_URL"] = "https://mock.firstock.in/V1"
    os.environ["FIRSTOCK_VENDOR_CODE"] = "TEST1_API"
    os.environ["FIRSTOCK_TOTP"] = "123456"

    class FakeClient:
        jkey = "testjkey"
        def login(self, password, totp):
            assert password == hashlib.sha256(b"testpass").hexdigest(), password
            assert totp == "123456"
            return {"data": {"susertoken": "testjkey"}}
        def get_balance(self):
            return {"data": {"cash": 1000.0, "marginused": 200.0, "collateral": 0}}
        def get_holdings(self):
            return {"data": [{"holdQuantity": 2, "uploadPrice": "100.00",
                              "exchangeTradingSymbol": [{"exchange": "NSE", "token": "2885", "tradingSymbol": "RELIANCE-EQ"}]}]}
        def get_positions(self):
            return {"data": [{"tradingSymbol": "RELIANCE-EQ", "netQuantity": 1, "netAveragePrice": "100.00",
                              "lastTradedPrice": "105.50", "product": "M", "exchange": "NSE", "token": "2885"}]}
        def get_order_book(self):
            return {"data": [{"orderNumber": "O123", "exchange": "NSE", "status": "FILLED",
                              "transactionType": "B", "product": "C", "priceType": "MKT",
                              "quantity": 1, "filledShares": 1, "price": "1885.00",
                              "tradingSymbol": "INFY-EQ", "token": "1594"}]}
        def get_trade_book(self):
            return {"data": [{"fillId": "T1", "fillPrice": "1885.00", "fillQuantity": 1, "orderNumber": "O123",
                              "tradingSymbol": "INFY-EQ", "transactionType": "B", "exchange": "NSE"}]}
        def place_order(self, payload):
            assert payload["tradingSymbol"] == "INFY-EQ", payload
            assert payload["priceType"] == "MKT", payload
            assert payload["mkt_protection"] == "1", payload
            assert payload["transactionType"] == "B", payload
            assert payload["exchange"] == "NSE", payload
            return {"data": {"orderNumber": "O123"}}
        def get_multi_quotes_ltp(self, items):
            assert items, items
            return {"data": [{"tradingSymbol": "RELIANCE-EQ", "lastTradedPrice": "105.50"}]}

    broker = BrokerConnector()
    broker.obj = FakeClient()
    broker.access_token = "testjkey"
    broker.user = "TEST1"
    session = broker.get_session()
    assert session["token"] == "testjkey"

    bal = broker.get_account_balance(session)
    assert bal["free_cash"] == 1000.0 and bal["source"] == "FIRSTOCK", bal
    assert bal["holdings"][0]["symbol"] == "RELIANCE-EQ", bal

    book = broker.get_order_book(session)
    row = book["raw"]["data"][0]
    assert row["orderid"] == "O123" and row["orderstatus"] == "complete", row

    placed = broker.place_order(session, symbol="INFY", side="BUY", qty=1,
                                order_type="MARKET", wait_for_confirmation=False)
    assert placed["order_id"] == "O123", placed

    ltp = broker._get_bulk_broker_ltp(["RELIANCE", "INFY"], session=session)
    assert ltp == {"RELIANCE": 105.5}, ltp

    print("[broker_firstock] self-check OK")
