"""
Firstock live LTP — polls the real quotes endpoint (getMultiQuotes/ltp) on the
same broker session. Firstock rate-limits quote calls (~1 req/sec), so all
subscribed symbols are fetched in ONE bulk request per poll cycle.
"""

import threading
import time
from typing import Callable, Iterable, Optional


class FirstockLTPPoller:
    """Same public API as TradeXLTPPoller / LiveLTPStream (start/stop/subscribe/unsubscribe)."""

    def __init__(
        self,
        broker,
        on_tick: Callable[[str, float, float], None],
        poll_interval: float = 2.0,
    ):
        self.broker = broker
        self.on_tick = on_tick
        self.poll_interval = poll_interval
        self._symbols: set = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_prices: dict = {}
        self._session = None

    def start(self, symbols: Iterable[str]) -> None:
        sym_list = [s.upper() for s in symbols]
        print(f"[LTP-FIRSTOCK] start() polling {len(sym_list)} symbols: {sym_list}")
        with self._lock:
            self._symbols.update(sym_list)
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._poll_loop, name="FirstockLTPPoller", daemon=True)
        self._thread.start()

    def subscribe(self, symbol: str) -> None:
        with self._lock:
            self._symbols.add(symbol.upper())

    def unsubscribe(self, symbol: str) -> None:
        with self._lock:
            self._symbols.discard(symbol.upper())

    def stop(self) -> None:
        self._stop.set()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                if self._session is None:
                    self._session = self.broker.get_session()
                with self._lock:
                    symbols = list(self._symbols)
                if not symbols:
                    time.sleep(self.poll_interval)
                    continue

                ltp_map = self.broker._get_bulk_broker_ltp(symbols, session=self._session)
                now = time.time()
                for sym, price in ltp_map.items():
                    prev = self._last_prices.get(sym)
                    if prev is None or abs(prev - price) > 1e-9:
                        self._last_prices[sym] = price
                        self.on_tick(sym, price, now)
            except Exception as e:
                print(f"[LTP-FIRSTOCK] poll error: {e}")
                self._session = None
            time.sleep(self.poll_interval)
