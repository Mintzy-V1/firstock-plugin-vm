"""
Broker factory — Firstock VM. Single credential set per session.

Legacy BROKER_TRADEX / BROKER_ANGEL names are kept as aliases of BROKER_FIRSTOCK
so the shared trading engine (api_server, auto_trader, session_manager,
orderbook) keeps working unchanged — this VM only ever talks to Firstock.
"""

import os
from typing import Any, Dict, Optional, Tuple

BROKER_FIRSTOCK = "firstock"
BROKER_TRADEX = BROKER_FIRSTOCK  # ponytail: legacy names route to FIRSTOCK in this VM
BROKER_ANGEL = BROKER_FIRSTOCK
SUPPORTED_BROKERS = {BROKER_FIRSTOCK}

DEFAULT_FIRSTOCK_BASE_URL = "https://api.firstock.in/V1"
DEFAULT_TRADEX_BASE_URL = DEFAULT_FIRSTOCK_BASE_URL  # ponytail: legacy alias kept for api_server import


def normalize_broker_type(broker_type: Optional[str]) -> str:
    bt = (broker_type or BROKER_FIRSTOCK).lower().strip()
    return bt if bt in SUPPORTED_BROKERS else BROKER_FIRSTOCK


def create_broker_connector(broker_type: Optional[str] = None, require_totp: bool = True):
    bt = normalize_broker_type(broker_type)
    from broker_firstock import BrokerConnector
    return BrokerConnector(require_totp=require_totp)


def set_broker_env(broker_config: Dict[str, Any]) -> str:
    """Set process env vars for the requested broker. Returns normalized broker_type."""
    bt = normalize_broker_type(broker_config.get("broker_type"))
    os.environ["FIRSTOCK_USER_ID"] = (
        broker_config.get("user_id_broker")
        or broker_config.get("user_id")
        or broker_config.get("client_code", "")
    )
    os.environ["FIRSTOCK_API_KEY"] = broker_config.get("api_key") or ""
    os.environ["FIRSTOCK_PASSWORD"] = broker_config.get("password") or ""
    # ponytail: FE proxy drops vendor_code; Firstock convention is {client_code}_API
    os.environ["FIRSTOCK_VENDOR_CODE"] = (
        broker_config.get("vendor_code")
        or f"{broker_config.get('client_code') or ''}_API"
    )
    os.environ["FIRSTOCK_BASE_URL"] = broker_config.get("base_url") or DEFAULT_FIRSTOCK_BASE_URL
    if broker_config.get("websocket_url"):
        os.environ["FIRSTOCK_MESSAGE_SOCKET"] = broker_config.get("websocket_url")
    if broker_config.get("totp") is not None:
        os.environ["FIRSTOCK_TOTP"] = broker_config.get("totp") or ""
    return bt


def clear_broker_env(broker_type: Optional[str] = None) -> None:
    for key in [
        "FIRSTOCK_USER_ID", "FIRSTOCK_API_KEY", "FIRSTOCK_PASSWORD",
        "FIRSTOCK_VENDOR_CODE", "FIRSTOCK_BASE_URL", "FIRSTOCK_MESSAGE_SOCKET",
        "FIRSTOCK_TOTP",
    ]:
        os.environ.pop(key, None)


def broker_config_from_session(session_data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "broker_type": session_data.get("broker_type", BROKER_FIRSTOCK),
        "api_key": session_data.get("api_key"),
        "client_code": session_data.get("client_code"),
        "password": session_data.get("password"),
        "user_id_broker": session_data.get("user_id_broker"),
        "vendor_code": session_data.get("vendor_code"),
        "base_url": session_data.get("base_url"),
        "websocket_url": session_data.get("websocket_url"),
        "broker_session": session_data.get("broker_session"),
        "totp": session_data.get("totp"),
    }


def requires_totp(broker_type: Optional[str]) -> bool:
    """Firstock login includes the TOTP (authenticator code) in the /login call."""
    return normalize_broker_type(broker_type) == BROKER_FIRSTOCK


def send_broker_otp(broker_config: Dict[str, Any]) -> None:
    """Firstock has no send-otp endpoint; TOTP is an authenticator code the user owns.
    Kept as a no-op so the api_server two-step credentials->totp flow is unchanged."""
    bt = set_broker_env(broker_config)
    try:
        broker = create_broker_connector(bt, require_totp=False)
        broker.send_otp()
    finally:
        clear_broker_env(bt)


def connect_broker(
    broker_config: Dict[str, Any],
    *,
    totp: Optional[str] = None,
    restore: bool = False,
) -> Tuple[Any, Dict[str, Any]]:
    """
    Create broker, login or restore session. Returns (broker, session_dict).
    """
    cfg = dict(broker_config)
    if totp is not None:
        cfg["totp"] = totp
    bt = set_broker_env(cfg)
    try:
        need_totp = requires_totp(bt) and not restore
        broker = create_broker_connector(bt, require_totp=need_totp)
        if restore and cfg.get("broker_session"):
            session = broker.restore_session(cfg["broker_session"])
        else:
            session = broker.get_session()
        return broker, session
    finally:
        clear_broker_env(bt)


def create_ltp_stream(broker, on_tick, market_client=None):
    from live_ltp_ws_firstock import FirstockLTPPoller
    return FirstockLTPPoller(broker, on_tick, market_client=market_client)
