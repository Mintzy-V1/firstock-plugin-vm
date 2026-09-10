
import os
import time
import json
import redis
import threading
import multiprocessing as mp
from multiprocessing import Process, Queue, Event, Manager
from queue import Empty
from typing import Dict, Any, Optional
from datetime import datetime
import traceback

from auto_trader import AutoTrader as AutoTraderA
from auto_trader_exposure_expansion import AutoTrader as AutoTraderB
from alerts import AlertManager
from client import PredictionClient, MarketClient
from broker_factory import (
    broker_config_from_session,
    create_ltp_stream,
    connect_broker,
    BROKER_TRADEX,
    BROKER_FIRSTOCK,
)
import signal

SIMULATION_STOP_PREFIX = "autotrader:simulation_stop:"
SIMULATION_STOP_TTL = 300
PYRAMID_RESULT_PREFIX = "autotrader:pyramid_result:"
PYRAMID_RESULT_TTL = 600
STOP_JOB_PREFIX = "autotrader:stop_job:"
STOP_JOB_TTL = 600

# Grace period for concurrent live-start when worker meta is missing (legacy / race).
LIVE_START_WORKER_GRACE_SECONDS = int(os.environ.get("LIVE_START_WORKER_GRACE_SECONDS", "60"))


class LiveStartPrepResult:
    """Outcome of prepare_for_live_start — do not kill an existing live worker."""
    CLEAR = "clear"
    PAPER_CLEARED = "paper_cleared"
    LIVE_ALREADY_RUNNING = "live_already_running"
    NOT_CLEARED = "not_cleared"

# Live handoff is blocked unless pyramid sets live_allowed=True explicitly.
_PYRAMID_HANDOFF_BLOCKED = {
    "applied": False,
    "live_allowed": False,
    "reason": "pyramid_not_run",
    "profitable_count": 0,
    "symbols_for_live": [],
}


def _signal_trader_stop(trader, stop_event, session_id: str, reason: str) -> None:
    """
    First action on any stop path: halt the trader loop before pyramid/shutdown.
    Sets trader threading.Event first, then the worker multiprocessing.Event.
    Both .set() calls are idempotent.
    """
    if trader is not None and hasattr(trader, "stop_event"):
        trader.stop_event.set()
        print(f"[Worker-{session_id}] trader.stop_event set ({reason})")
    stop_event.set()


class WorkerProcess:
    """Wrapper for a trader process with health monitoring"""
    
    def __init__(self, session_id: str, process: Process, 
                 stop_event: Event, health_queue: Queue):
        self.session_id = session_id
        self.process = process
        self.stop_event = stop_event
        self.health_queue = health_queue
        self.last_heartbeat = time.time()
        self.started_at = time.time()
        
    def is_alive(self) -> bool:
        return self.process.is_alive()
    
    def is_healthy(self, timeout: int = 60) -> bool:
        """Check if worker sent heartbeat recently"""
        return (time.time() - self.last_heartbeat) < timeout
    
    def update_heartbeat(self):
        self.last_heartbeat = time.time()


def _trader_worker(
    session_id: str,
    strategy: str,
    symbols: list,
    allocations: dict,
    time_frame: str,
    candle: str,
    broker_config: dict,
    trading_logs_collection_name: str,
    stop_event: Event,
    health_queue: Queue,
    mongo_uri: str,
    mongo_db_name: str,
    configuration_id: Optional[str] = None,
    mongo_config_db_name: Optional[str] = None,
    leverage_multiplier: Optional[float] = None,
    simulation_logs: bool = False,
):
    """
    Worker process that runs a single AutoTrader instance.
    Isolated from other traders - has its own Python interpreter and memory space.
    """
    import signal

    trader_holder: Dict[str, Any] = {"trader": None}

    def handle_sigterm(signum, frame):
        print(f"[Worker-{session_id}] SIGTERM mila — graceful shutdown...")
        _signal_trader_stop(trader_holder["trader"], stop_event, session_id, "SIGTERM")

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        broker, restored = connect_broker(broker_config, restore=True)
        os.environ["BROKER_TYPE"] = broker_config.get("broker_type", BROKER_TRADEX)
        
        if not restored or not restored.get("token"):
            health_queue.put({
                "session_id": session_id,
                "status": "error",
                "error": "Failed to restore broker session"
            })
            return
        
        # Set up MongoDB connection (each process gets its own connection)
        from pymongo import MongoClient
        mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        mongo_db = mongo_client[mongo_db_name]
        trading_logs_collection = mongo_db[trading_logs_collection_name]
        config_db_name = (
            mongo_config_db_name
            or os.environ.get("MONGO_CONFIG_DB_NAME")
            or "test"
        )
        os.environ["MONGO_CONFIG_DB_NAME"] = config_db_name
        print(
            f"[Worker-{session_id}] Mongo DBs — sessions/logs: {mongo_db_name}, "
            f"SavedTradingConfiguration: {config_db_name}"
        )
        
        # Initialize clients — ML predictions run on the model VM (port 8000 required)
        prediction_base_url = os.environ.get(
            "PREDICTION_API_URL",
            "http://54.204.215.28:8000/predict",
        ).strip()
        print(f"[Worker-{session_id}] PredictionClient -> {prediction_base_url}")
        prediction_client = PredictionClient(
            api_key=os.environ.get(
                "PREDICTION_API_KEY",
                "XeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            ),
            base_url=prediction_base_url,
        )
        market_client = MarketClient()
        
        # Redis client for inter-process messaging (single-symbol exit requests)
        exit_redis_client = getattr(market_client, 'redis_client', None)
        
        # Select trader class (C is lazy-imported so api_server boots even if org module differs on disk)
        TRADER_MAP = {"A": AutoTraderA, "B": AutoTraderB}
        if strategy == "C":
            try:
                from auto_trader_exposure_expansion_org import AutoTrader as AutoTraderC
                TRADER_MAP["C"] = AutoTraderC
            except ImportError as e:
                health_queue.put({
                    "session_id": session_id,
                    "status": "error",
                    "error": (
                        f"Strategy C (auto_trader_exposure_expansion_org) unavailable: {e}. "
                        "Ensure auto_trader_exposure_expansion_org.py defines class AutoTrader."
                    ),
                })
                return
        TraderClass = TRADER_MAP.get(strategy)
        
        if not TraderClass:
            health_queue.put({
                "session_id": session_id,
                "status": "error",
                "error": f"Invalid strategy: {strategy}"
            })
            return
        
        # Create trader instance
        trader = TraderClass(
            prediction_client=prediction_client,
            market_client=market_client,
            broker=broker,
            alerts=AlertManager(),
            trading_logs_collection=trading_logs_collection
        )
        trader.config_db_name = config_db_name
        
        # Configure trader
        trader.session_id = session_id
        trader.broker_live_session = restored
        trader.broker_session_payload = broker_config.get("broker_session")
        session_free_cash = broker_config.get("free_cash")
        if session_free_cash is not None:
            try:
                trader.session_free_cash = float(session_free_cash)
            except (TypeError, ValueError):
                pass
        trader.session = restored
        trader.ui_session_id = session_id
        trader.symbol_allocations = {k: v["capital"] for k, v in allocations.items()}
        trader.initial_allocations = allocations
        trader.configuration_id = configuration_id
        trader.simulation_logs = bool(simulation_logs)
        if leverage_multiplier is not None:
            try:
                trader.leverage_multiplier = float(leverage_multiplier)
            except (TypeError, ValueError):
                trader.leverage_multiplier = None

        trader_holder["trader"] = trader

        # Signal that we're healthy and starting
        health_queue.put({
            "session_id": session_id,
            "status": "starting",
            "timestamp": time.time()
        })
        
        # Start heartbeat thread
        def heartbeat_loop():
            while not stop_event.is_set():
                try:
                    health_queue.put({
                        "session_id": session_id,
                        "status": "running",
                        "timestamp": time.time()
                    }, timeout=1)
                except:
                    pass
                time.sleep(10)  # Heartbeat every 10 seconds
        
        heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        
        # Run the trader with monitoring
        print(f"[Worker-{session_id}] Starting trader with {len(symbols)} symbols")
        
        # Wrap trader.start in a monitoring loop
        trader_thread = threading.Thread(
            target=trader.start,
            args=(symbols, time_frame, candle),
            kwargs={
                "initial_allocations": allocations,
                "leverage_multiplier": leverage_multiplier,
            },
            daemon=False  # Don't make daemon - we want proper cleanup
        )
        trader_thread.start()

        # ---------- Background LTP stream (independent of PnL flow) ----------
        ltp_stream = None
        try:
            print(
                f"[Worker-{session_id}] Wiring LTP stream -> trader.on_ltp_tick "
                f"broker={getattr(broker, 'broker_type', 'unknown')} "
                f"symbols={list(allocations.keys())}"
            )
            ltp_stream = create_ltp_stream(
                broker,
                trader.on_ltp_tick,
                market_client=getattr(trader, "market_client", None),
            )
            ltp_stream.start(list(allocations.keys()))
            print(f"[Worker-{session_id}] LiveLTPStream started for {len(allocations)} symbols")
        except Exception as e:
            print(f"[Worker-{session_id}] LiveLTPStream failed to start: {e}")

        # Monitor for stop signal AND single-symbol exit requests
        exit_queue_key = f"autotrader:exit_request:{session_id}"
        sim_stop_key = f"{SIMULATION_STOP_PREFIX}{session_id}"
        while trader_thread.is_alive() and not stop_event.is_set():
            if exit_redis_client:
                try:
                    if exit_redis_client.get(sim_stop_key):
                        _signal_trader_stop(
                            trader, stop_event, session_id, "redis simulation_stop"
                        )
                        break
                except Exception as e:
                    print(f"[Worker-{session_id}] Simulation stop flag check error: {e}")

            # Check for single-symbol exit requests from Redis
            if exit_redis_client:
                try:
                    exit_req_raw = exit_redis_client.lpop(exit_queue_key)
                    if exit_req_raw:
                        data = json.loads(exit_req_raw)
                        exit_symbol = data.get("symbol", "")
                        print(f"[Worker-{session_id}] Exit request received for symbol: {exit_symbol}")
                        result = trader.exit_single_position(exit_symbol)
                        # Push result back to Redis for the API to read
                        result_key = f"autotrader:exit_result:{session_id}:{exit_symbol}"
                        exit_redis_client.setex(result_key, 60, json.dumps(result))
                        print(f"[Worker-{session_id}] Exit result for {exit_symbol}: {result}")
                except Exception as e:
                    print(f"[Worker-{session_id}] Exit queue check error: {e}")
            
            trader_thread.join(timeout=1)
        
        handoff = None
        # If stop was requested, shutdown trader
        if stop_event.is_set():
            print(f"[Worker-{session_id}] Stop requested, shutting down...")
            _signal_trader_stop(trader, stop_event, session_id, "worker stop monitor")
            trader_thread.join(timeout=45)
            if trader_thread.is_alive():
                print(
                    f"[Worker-{session_id}] trader_thread still alive after 45s "
                    "— proceeding with shutdown"
                )
            trader.shutdown()
            print("shutdown called successfully")
            trader_thread.join(timeout=60)

            handoff = getattr(trader, "_pyramid_handoff_result", None)
            if handoff is None:
                handoff = dict(_PYRAMID_HANDOFF_BLOCKED)
            if exit_redis_client:
                try:
                    exit_redis_client.setex(
                        f"{PYRAMID_RESULT_PREFIX}{session_id}",
                        PYRAMID_RESULT_TTL,
                        json.dumps(handoff),
                    )
                    print(
                        f"[Worker-{session_id}] Pyramid handoff stored in Redis "
                        f"(live_allowed={handoff.get('live_allowed')}, reason={handoff.get('reason')})"
                    )
                except Exception as e:
                    print(f"[Worker-{session_id}] Failed to store pyramid handoff in Redis: {e}")

        if ltp_stream is not None:
            try:
                ltp_stream.stop()
            except Exception as e:
                print(f"[Worker-{session_id}] LiveLTPStream stop error: {e}")


        
        simulation_stop = False
        try:
            if exit_redis_client:
                sim_key = f"{SIMULATION_STOP_PREFIX}{session_id}"
                simulation_stop = bool(exit_redis_client.get(sim_key))
                if simulation_stop:
                    exit_redis_client.delete(sim_key)
        except Exception as e:
            print(f"[Worker-{session_id}] Simulation stop flag check error: {e}")

        if simulation_stop:
            health_queue.put({
                "session_id": session_id,
                "status": "simulation_stopped",
                "timestamp": time.time()
            })
            try:
                sessions_collection = mongo_db["plugin_sessions"]
                sessions_collection.update_one(
                    {"session_id": session_id},
                    {
                        "$set": {
                            "trading_status": "simulation_stopped",
                            "simulation_stopped_at": datetime.utcnow(),
                            "last_updated": datetime.utcnow()
                        }
                    }
                )
                print(f"[Worker-{session_id}] Simulation stopped in Mongo (session auth unchanged)")
            except Exception as e:
                print(f"[Worker-{session_id}] Failed to mark simulation stopped in Mongo: {e}")

            if stop_event.is_set():
                try:
                    if handoff is None:
                        handoff = dict(_PYRAMID_HANDOFF_BLOCKED)
                    SessionManager.complete_stop_job(
                        session_id,
                        handoff,
                        simulation_stop=True,
                    )
                except Exception as job_err:
                    print(f"[Worker-{session_id}] Failed to update stop job in Redis: {job_err}")
        else:
            health_queue.put({
                "session_id": session_id,
                "status": "stopped",
                "timestamp": time.time()
            })

            try:
                sessions_collection = mongo_db["plugin_sessions"]
                sessions_collection.update_one(
                    {"session_id": session_id},
                    {
                        "$set": {
                            "status": "stopped",
                            "trading_status": "stopped",
                            "stopped_at": datetime.utcnow(),
                            "last_updated": datetime.utcnow()
                        }
                    }
                )
                print(f"[Worker-{session_id}] Session status marked stopped in Mongo")
            except Exception as e:
                print(f"[Worker-{session_id}] Failed to mark session stopped in Mongo: {e}")
        
        # Cleanup
        mongo_client.close()
        
    except Exception as e:
        error_msg = f"Worker error: {str(e)}\n{traceback.format_exc()}"
        print(f"[Worker-{session_id}] {error_msg}")
        try:
            existing_job = SessionManager.read_stop_job(session_id)
            if existing_job and existing_job.get("status") == "stopping":
                SessionManager.fail_stop_job(session_id, error_msg)
        except Exception:
            pass
        health_queue.put({
            "session_id": session_id,
            "status": "error",
            "error": error_msg,
            "timestamp": time.time()
        })


class SessionManager:
    """
    Optimized session manager using multiprocessing for true parallelism.
    Each trader runs in its own process with isolated resources.
    """
     
    _workers: Dict[str, WorkerProcess] = {}
    _manager = None
    _health_queue = None
    _monitor_thread = None
    _monitor_stop = threading.Event()
   
    @classmethod
    def _ensure_manager(cls):
        if cls._manager is None:
            from multiprocessing import Manager
            cls._manager = Manager()
            cls._health_queue = cls._manager.Queue()
   

    _market_client: MarketClient = MarketClient()
    
    @classmethod
    def _redis(cls):
        """Convenience accessor for MarketClient's Redis connection."""
        return cls._market_client.redis_client

    REDIS_KEY_PREFIX = "autotrader:session:" 
    REDIS_META_SUFFIX = ":meta"
    SESSION_REDIS_TTL = 86400
    PID_DIR = os.environ.get("AUTOTRADER_PID_DIR", "/tmp")
    SIMULATION_STOP_PREFIX = SIMULATION_STOP_PREFIX
    SIMULATION_STOP_TTL = SIMULATION_STOP_TTL
    PYRAMID_RESULT_PREFIX = PYRAMID_RESULT_PREFIX
    PYRAMID_RESULT_TTL = PYRAMID_RESULT_TTL
    STOP_JOB_PREFIX = STOP_JOB_PREFIX
    STOP_JOB_TTL = STOP_JOB_TTL

    @classmethod
    def _stop_job_key(cls, session_id: str) -> str:
        return f"{cls.STOP_JOB_PREFIX}{session_id}"

    @classmethod
    def read_stop_job(cls, session_id: str) -> Optional[Dict[str, Any]]:
        try:
            raw = cls._redis().get(cls._stop_job_key(session_id))
            if not raw:
                return None
            return json.loads(raw)
        except Exception as e:
            print(f"[SessionManager] Failed to read stop job for {session_id}: {e}")
            return None

    @classmethod
    def init_stop_job(cls, session_id: str, **fields) -> Dict[str, Any]:
        job = {
            "session_id": session_id,
            "status": "stopping",
            "phase": "queued",
            "started_at": time.time(),
            "completed_at": None,
            "live_allowed": None,
            "pyramid": None,
            "error": None,
            "finalized": False,
            "response": None,
        }
        job.update(fields)
        try:
            cls._redis().setex(
                cls._stop_job_key(session_id),
                cls.STOP_JOB_TTL,
                json.dumps(job),
            )
            print(f"[SessionManager] Stop job initialized for {session_id} status={job['status']}")
        except Exception as e:
            print(f"[SessionManager] Failed to init stop job for {session_id}: {e}")
        return job

    @classmethod
    def update_stop_job(cls, session_id: str, **fields) -> Optional[Dict[str, Any]]:
        job = cls.read_stop_job(session_id) or {
            "session_id": session_id,
            "started_at": time.time(),
            "status": "stopping",
            "phase": "queued",
        }
        job.update(fields)
        try:
            cls._redis().setex(
                cls._stop_job_key(session_id),
                cls.STOP_JOB_TTL,
                json.dumps(job),
            )
        except Exception as e:
            print(f"[SessionManager] Failed to update stop job for {session_id}: {e}")
            return None
        return job

    @classmethod
    def complete_stop_job(
        cls,
        session_id: str,
        handoff: Dict[str, Any],
        *,
        simulation_stop: bool = True,
    ) -> None:
        cls.update_stop_job(
            session_id,
            status="completed",
            phase="done",
            completed_at=time.time(),
            live_allowed=bool(handoff.get("live_allowed", False)),
            pyramid=handoff,
            simulation_stop=simulation_stop,
            trading_status=(
                "simulation_stopped"
                if simulation_stop and bool(handoff.get("live_allowed", False))
                else "stopped"
            ),
        )
        print(
            f"[SessionManager] Stop job completed for {session_id} "
            f"live_allowed={handoff.get('live_allowed')}"
        )

    @classmethod
    def fail_stop_job(cls, session_id: str, error: str) -> None:
        cls.update_stop_job(
            session_id,
            status="failed",
            phase="failed",
            completed_at=time.time(),
            error=str(error)[:2000],
        )
        print(f"[SessionManager] Stop job failed for {session_id}: {error}")

    @classmethod
    def _pyramid_result_key(cls, session_id: str) -> str:
        return f"{cls.PYRAMID_RESULT_PREFIX}{session_id}"

    @classmethod
    def read_pyramid_handoff_result(cls, session_id: str):
        try:
            raw = cls._redis().get(cls._pyramid_result_key(session_id))
            if not raw:
                return None
            return json.loads(raw)
        except Exception as e:
            print(f"[SessionManager] Failed to read pyramid handoff for {session_id}: {e}")
            return None

    @classmethod
    def clear_pyramid_handoff_result(cls, session_id: str) -> None:
        try:
            cls._redis().delete(cls._pyramid_result_key(session_id))
        except Exception as e:
            print(f"[SessionManager] Failed to clear pyramid handoff for {session_id}: {e}")

    @classmethod
    def _simulation_stop_key(cls, session_id: str) -> str:
        return f"{cls.SIMULATION_STOP_PREFIX}{session_id}"

    @classmethod
    def _mark_simulation_stop(cls, session_id: str) -> bool:
        try:
            cls._redis().setex(
                cls._simulation_stop_key(session_id),
                cls.SIMULATION_STOP_TTL,
                "1",
            )
            print(f"[SessionManager] Simulation stop flag set for {session_id}")
            return True
        except Exception as e:
            print(f"[SessionManager] Failed to set simulation stop flag: {e}")
            return False

    @classmethod
    def _pid_alive(cls, pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    @classmethod
    def _get_redis_pid(cls, session_id: str) -> Optional[int]:
        try:
            redis_client = cls._redis()
            if not redis_client:
                return None
            pid_str = redis_client.get(f"{cls.REDIS_KEY_PREFIX}{session_id}")
            return int(pid_str) if pid_str else None
        except Exception:
            return None

    @classmethod
    def _session_meta_key(cls, session_id: str) -> str:
        return f"{cls.REDIS_KEY_PREFIX}{session_id}{cls.REDIS_META_SUFFIX}"

    @classmethod
    def _get_session_worker_meta(cls, session_id: str) -> Optional[Dict[str, Any]]:
        try:
            raw = cls._redis().get(cls._session_meta_key(session_id))
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            return None

    @classmethod
    def _set_session_worker_meta(cls, session_id: str, strategy: str, pid: int) -> None:
        meta = {
            "strategy": strategy,
            "pid": pid,
            "started_at": time.time(),
        }
        try:
            cls._redis().setex(
                cls._session_meta_key(session_id),
                cls.SESSION_REDIS_TTL,
                json.dumps(meta),
            )
        except Exception as e:
            print(f"[SessionManager] Redis meta save failed for {session_id}: {e}")

    @classmethod
    def _delete_session_redis_keys(cls, session_id: str) -> None:
        try:
            redis_client = cls._redis()
            if not redis_client:
                return
            redis_client.delete(
                f"{cls.REDIS_KEY_PREFIX}{session_id}",
                cls._session_meta_key(session_id),
            )
        except Exception as e:
            print(f"[SessionManager] Redis delete failed for {session_id}: {e}")

    @classmethod
    def _is_live_worker_meta(cls, meta: Optional[Dict[str, Any]]) -> bool:
        if not meta:
            return False
        strategy = meta.get("strategy")
        if strategy and strategy != "B":
            return True
        started_at = meta.get("started_at")
        if strategy is None and started_at is not None:
            try:
                age = time.time() - float(started_at)
                if age < LIVE_START_WORKER_GRACE_SECONDS:
                    return True
            except (TypeError, ValueError):
                pass
        return False

    @classmethod
    def _reconcile_session_registry(cls, session_id: str) -> None:
        """
        Drop stale local/Redis/PID-file worker entries before start/stop decisions.
        This keeps simulation-to-live handoff from being blocked by dead local
        worker objects or stale PID tracking.
        """
        worker = cls._workers.get(session_id)
        redis_pid = cls._get_redis_pid(session_id)

        if worker and not worker.is_alive():
            print(f"[SessionManager] Reconcile: dead local worker for {session_id}")
            cls._cleanup_worker(session_id)
            worker = None

        if worker and worker.is_alive():
            local_pid = worker.process.pid
            if redis_pid is None:
                print(
                    f"[SessionManager] Reconcile: local worker PID={local_pid} for {session_id} "
                    "but Redis entry is missing - cleaning local worker"
                )
                worker.stop_event.set()
                worker.process.join(timeout=5)
                if worker.is_alive():
                    try:
                        worker.process.terminate()
                        worker.process.join(timeout=3)
                    except Exception:
                        pass
                cls._cleanup_worker(session_id)
            elif redis_pid != local_pid:
                print(
                    f"[SessionManager] Reconcile: PID mismatch local={local_pid} redis={redis_pid} "
                    f"for {session_id} - cleaning local registry"
                )
                cls._cleanup_worker(session_id)

        redis_pid = cls._get_redis_pid(session_id)
        if redis_pid is not None and not cls._pid_alive(redis_pid):
            print(f"[SessionManager] Reconcile: Redis PID={redis_pid} for {session_id} is dead - clearing")
            cls._delete_session_redis_keys(session_id)
            cls._workers.pop(session_id, None)

        file_pid = cls._read_worker_pid_file(session_id)
        if file_pid is not None and not cls._pid_alive(file_pid):
            print(f"[SessionManager] Reconcile: PID file PID={file_pid} for {session_id} is dead - clearing")
            cls._clear_worker_pid_file(session_id)
            cls._clear_worker_pid(session_id)

    @classmethod
    def _wait_for_pid_exit(cls, pid: int, timeout: float = 90.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not cls._pid_alive(pid):
                return True
            time.sleep(0.5)
        return not cls._pid_alive(pid)

    @classmethod
    def _terminate_session_worker(cls, session_id: str, timeout: float = 90.0) -> bool:
        """Terminate session worker (local registry + Redis PID + fallback PID file)."""
        worker = cls._workers.get(session_id)
        if worker and worker.is_alive():
            print(f"[SessionManager] _terminate_session_worker: signaling local worker {session_id}")
            worker.stop_event.set()
            worker.process.join(timeout=min(timeout, 60))
            if worker.is_alive():
                try:
                    worker.process.terminate()
                    worker.process.join(timeout=5)
                except Exception:
                    pass
            cls._cleanup_worker(session_id)

        redis_pid = cls._get_redis_pid(session_id)
        if redis_pid and cls._pid_alive(redis_pid):
            print(f"[SessionManager] _terminate_session_worker: terminating PID={redis_pid} for {session_id}")
            cls._terminate_pid(redis_pid)
            if not cls._wait_for_pid_exit(redis_pid, timeout):
                print(f"[SessionManager] _terminate_session_worker: PID={redis_pid} still alive for {session_id}")
                return False

        fallback_pid = cls._resolve_worker_pid(session_id)
        if fallback_pid and fallback_pid != redis_pid and cls._pid_alive(fallback_pid):
            print(f"[SessionManager] _terminate_session_worker: terminating fallback PID={fallback_pid} for {session_id}")
            cls._terminate_pid(fallback_pid)
            if not cls._wait_for_pid_exit(fallback_pid, timeout):
                return False

        cls._delete_session_redis_keys(session_id)
        cls._clear_worker_pid(session_id)
        cls._clear_worker_pid_file(session_id)

        cls._reconcile_session_registry(session_id)
        redis_pid = cls._get_redis_pid(session_id)
        final_pid = cls._resolve_worker_pid(session_id)
        still_running = session_id in cls._workers or (
            (redis_pid is not None and cls._pid_alive(redis_pid))
            or (final_pid is not None and cls._pid_alive(final_pid))
        )
        return not still_running

    @classmethod
    def prepare_for_live_start(cls, session_id: str, timeout: float = 90.0) -> str:
        """
        Before spawning a live worker: clear paper (B) only.
        Never SIGTERM an existing live (non-B) worker — concurrent live starts are idempotent.
        """
        cls._reconcile_session_registry(session_id)

        redis_pid = cls._get_redis_pid(session_id)
        meta = cls._get_session_worker_meta(session_id)

        if redis_pid and cls._pid_alive(redis_pid):
            if cls._is_live_worker_meta(meta):
                print(
                    f"[SessionManager] prepare_for_live_start: LIVE_ALREADY_RUNNING "
                    f"session={session_id} pid={redis_pid} strategy={meta.get('strategy') if meta else None}"
                )
                return LiveStartPrepResult.LIVE_ALREADY_RUNNING

            strategy = (meta or {}).get("strategy")
            print(
                f"[SessionManager] prepare_for_live_start: terminating paper/stale worker "
                f"session={session_id} pid={redis_pid} strategy={strategy or 'unknown'}"
            )
            if cls._terminate_session_worker(session_id, timeout):
                print(f"[SessionManager] prepare_for_live_start: PAPER_CLEARED session={session_id}")
                return LiveStartPrepResult.PAPER_CLEARED
            print(f"[SessionManager] prepare_for_live_start: NOT_CLEARED session={session_id}")
            return LiveStartPrepResult.NOT_CLEARED

        worker = cls._workers.get(session_id)
        if worker and worker.is_alive():
            meta = cls._get_session_worker_meta(session_id)
            if cls._is_live_worker_meta(meta):
                print(
                    f"[SessionManager] prepare_for_live_start: LIVE_ALREADY_RUNNING (local) "
                    f"session={session_id} pid={worker.process.pid}"
                )
                return LiveStartPrepResult.LIVE_ALREADY_RUNNING
            if cls._terminate_session_worker(session_id, timeout):
                return LiveStartPrepResult.PAPER_CLEARED
            return LiveStartPrepResult.NOT_CLEARED

        print(f"[SessionManager] prepare_for_live_start: CLEAR session={session_id}")
        return LiveStartPrepResult.CLEAR

    @classmethod
    def ensure_worker_stopped(cls, session_id: str, timeout: float = 90.0) -> bool:
        """Block until no worker remains for this session (paper stop / full cleanup)."""
        cls._reconcile_session_registry(session_id)
        cleared = cls._terminate_session_worker(session_id, timeout)
        if not cleared:
            print(f"[SessionManager] ensure_worker_stopped: worker still alive for {session_id}")
            return False
        print(f"[SessionManager] ensure_worker_stopped: {session_id} is clear")
        return True


    
    # Configuration
    MAX_WORKERS = int(os.environ.get("MAX_TRADER_WORKERS", "6"))  # Limit concurrent processes
    HEALTH_CHECK_INTERVAL = 30  # seconds
    WORKER_TIMEOUT = 120  # seconds without heartbeat = dead
    
    @classmethod
    def _start_monitor(cls):
        """Start background thread to monitor worker health"""
        cls._ensure_manager()
        if cls._monitor_thread and cls._monitor_thread.is_alive():
            return
        
        def monitor_loop():
            print("[SessionManager] Health monitor started")
            while not cls._monitor_stop.is_set():
                try:
                    # Process health updates
                    while True:
                        try:
                            msg = cls._health_queue.get(timeout=0.1)
                            session_id = msg.get("session_id")
                            status = msg.get("status")
                            
                            if session_id in cls._workers:
                                worker = cls._workers[session_id]
                                worker.update_heartbeat()
                                
                                if status == "error":
                                    print(f"[SessionManager] Worker {session_id} reported error: {msg.get('error')}")
                                elif status == "stopped":
                                    print(f"[SessionManager] Worker {session_id} stopped gracefully")
                                elif status == "simulation_stopped":
                                    print(f"[SessionManager] Worker {session_id} simulation stopped (session auth kept)")
                        
                        except Empty:
                            break
                    
                    # Check for unhealthy workers
                    dead_workers = []
                    for session_id, worker in cls._workers.items():
                        if not worker.is_alive():
                            print(f"[SessionManager] Worker {session_id} process died")
                            dead_workers.append(session_id)
                        elif not worker.is_healthy(cls.WORKER_TIMEOUT):
                            print(f"[SessionManager] Worker {session_id} stopped responding (timeout)")
                            dead_workers.append(session_id)
                    
                    # Clean up dead workers
                    for session_id in dead_workers:
                        cls._cleanup_worker(session_id)
                    
                except Exception as e:
                    print(f"[SessionManager] Monitor error: {e}")
                
                time.sleep(cls.HEALTH_CHECK_INTERVAL)
        
        cls._monitor_stop.clear()
        cls._monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        cls._monitor_thread.start()
    
    @classmethod
    def _cleanup_worker(cls, session_id: str):
        """Clean up a dead or stopped worker"""
        worker = cls._workers.get(session_id)
        if not worker:
            return
        
        try:
            if worker.is_alive():
                worker.stop_event.set()
                worker.process.join(timeout=5)
                
                if worker.process.is_alive():
                    print(f"[SessionManager] Force terminating worker {session_id}")
                    worker.process.terminate()
                    worker.process.join(timeout=2)
                    
                    if worker.process.is_alive():
                        worker.process.kill()
        except Exception as e:
            print(f"[SessionManager] Error cleaning up worker {session_id}: {e}")
        finally:
            cls._workers.pop(session_id, None)
    
    @classmethod
    def _pid_file_path(cls, session_id: str) -> str:
        safe_id = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in session_id)
        return os.path.join(cls.PID_DIR, f"autotrader_{safe_id}.pid")

    @classmethod
    def _save_worker_pid_file(cls, session_id: str, pid: int) -> None:
        try:
            with open(cls._pid_file_path(session_id), "w", encoding="utf-8") as handle:
                handle.write(str(pid))
        except Exception as e:
            print(f"[SessionManager] PID file save failed: {e}")

    @classmethod
    def _read_worker_pid_file(cls, session_id: str) -> Optional[int]:
        try:
            path = cls._pid_file_path(session_id)
            if not os.path.exists(path):
                return None
            with open(path, "r", encoding="utf-8") as handle:
                return int(handle.read().strip())
        except Exception as e:
            print(f"[SessionManager] PID file read failed: {e}")
            return None

    @classmethod
    def _clear_worker_pid_file(cls, session_id: str) -> None:
        try:
            os.remove(cls._pid_file_path(session_id))
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"[SessionManager] PID file delete failed: {e}")

    @classmethod
    def _mongo_settings(cls):
        mongo_uri = os.environ.get(
            "MONGO_URI",
            "mongodb+srv://mintzy01ai_db_user:zTqQRkovgKbLXQdp@cluster0.cztcxpr.mongodb.net/?appName=Cluster0",
        )
        mongo_db_name = os.environ.get("MONGO_DB_NAME", "mintzy_plugin")
        return mongo_uri, mongo_db_name

    @classmethod
    def _save_worker_pid(cls, session_id: str, pid: int) -> None:
        try:
            from pymongo import MongoClient

            mongo_uri, mongo_db_name = cls._mongo_settings()
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client[mongo_db_name]["plugin_sessions"].update_one(
                {"session_id": session_id},
                {"$set": {"worker_pid": pid, "last_updated": datetime.utcnow()}},
            )
            client.close()
            print(f"[SessionManager] Mongo worker_pid saved session={session_id} pid={pid}")
        except Exception as e:
            print(f"[SessionManager] Mongo worker_pid save failed: {e}")

    @classmethod
    def _clear_worker_pid(cls, session_id: str) -> None:
        try:
            from pymongo import MongoClient

            mongo_uri, mongo_db_name = cls._mongo_settings()
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client[mongo_db_name]["plugin_sessions"].update_one(
                {"session_id": session_id},
                {"$set": {"worker_pid": None, "last_updated": datetime.utcnow()}},
            )
            client.close()
        except Exception as e:
            print(f"[SessionManager] Mongo worker_pid clear failed: {e}")

    @classmethod
    def _resolve_worker_pid(cls, session_id: str) -> Optional[int]:
        redis_client = cls._redis()
        if redis_client:
            try:
                pid_str = redis_client.get(f"{cls.REDIS_KEY_PREFIX}{session_id}")
                if pid_str:
                    return int(pid_str)
            except Exception as e:
                print(f"[SessionManager] Redis PID lookup failed: {e}")

        pid = cls._read_worker_pid_file(session_id)
        if pid:
            return pid

        try:
            from pymongo import MongoClient

            mongo_uri, mongo_db_name = cls._mongo_settings()
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            doc = client[mongo_db_name]["plugin_sessions"].find_one(
                {"session_id": session_id},
                {"worker_pid": 1},
            )
            client.close()
            if doc and doc.get("worker_pid"):
                return int(doc["worker_pid"])
        except Exception as e:
            print(f"[SessionManager] Mongo PID lookup failed: {e}")

        return None

    @classmethod
    def _terminate_pid(cls, pid: int) -> bool:
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"[SessionManager] SIGTERM sent to PID={pid}")

            try:
                import psutil
            except ImportError:
                print("[SessionManager] psutil not installed - polling PID until exit")
                if not cls._wait_for_pid_exit(pid, timeout=90):
                    print(f"[SessionManager] PID={pid} still alive after 90s, sending SIGKILL")
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    return cls._wait_for_pid_exit(pid, timeout=10)
                return True

            try:
                proc = psutil.Process(pid)
                proc.wait(timeout=60)
                print(f"[SessionManager] Process {pid} exited gracefully")
            except psutil.TimeoutExpired:
                print(f"[SessionManager] PID={pid} still alive after 60s, sending SIGKILL")
                os.kill(pid, signal.SIGKILL)
                return cls._wait_for_pid_exit(pid, timeout=10)
            except psutil.NoSuchProcess:
                print(f"[SessionManager] Process {pid} already exited")
            return True
        except ProcessLookupError:
            print(f"[SessionManager] PID={pid} does not exist (already stopped)")
            return True
        except Exception as e:
            print(f"[SessionManager] Failed to terminate PID={pid}: {e}")
            return False

    @classmethod
    def start_session(cls, session_id: str, session_doc: dict, trading_logs_collection):
        """Start a new trading session in an isolated process"""
        
        # Start monitor if not running
        cls._start_monitor()

        cls._reconcile_session_registry(session_id)

        # Check if session already exists
        if session_id in cls._workers:
            worker = cls._workers[session_id]
            if not worker.is_alive():
                print(f"[SessionManager] Stale worker registry for {session_id} - cleaning up")
                cls._cleanup_worker(session_id)
            else:
                print(f"[SessionManager] Session already running: {session_id}")
                raise RuntimeError(f"Session {session_id} is already running")

        redis_pid = cls._get_redis_pid(session_id)
        if redis_pid is not None and cls._pid_alive(redis_pid):
            print(f"[SessionManager] Session already running (Redis PID={redis_pid}): {session_id}")
            raise RuntimeError(f"Session {session_id} is already running")

        # Check worker limit
        active_workers = sum(1 for w in cls._workers.values() if w.is_alive())
        if active_workers >= cls.MAX_WORKERS:
            raise RuntimeError(
                f"Maximum concurrent sessions ({cls.MAX_WORKERS}) reached. "
                f"Stop a session or increase MAX_TRADER_WORKERS environment variable."
            )
        
        # Parse session configuration
        strategy = session_doc.get("strategy", "A")
        raw_symbols = session_doc.get("symbols", [])
        
        symbols = []
        allocations = {}
        
        for s in raw_symbols:
            if not isinstance(s, dict):
                continue
            
            sym = s.get("symbol")
            cap = float(s.get("capital", 0))
            sl = float(s.get("stop_loss", 0.02))
            
            if sym:
                symbols.append(sym)
                allocations[sym] = {
                    "capital": cap,
                    "stop_loss": sl
                }
        
        if not symbols:
            raise RuntimeError("CRITICAL: Empty symbols list")
        
        print(f"[SessionManager] Starting session {session_id}")
        print(f"  Strategy: {strategy}")
        print(f"  Symbols: {symbols}")
        print(f"  Allocations: {allocations}")
        
        time_frame = session_doc.get("time_frame", "5 minutes")
        candle = session_doc.get("candle", "5m")
        configuration_id = session_doc.get("configuration_id")
        leverage_multiplier = session_doc.get("leverage_multiplier")
        try:
            leverage_multiplier = float(leverage_multiplier) if leverage_multiplier is not None else 1.0
            if leverage_multiplier <= 0:
                leverage_multiplier = 1.0
        except (TypeError, ValueError):
            leverage_multiplier = 1.0
        simulation_logs = bool(session_doc.get("simulation_logs", False))
        print(f"  Leverage multiplier: {leverage_multiplier}")
        if simulation_logs:
            print("  Mode: paper simulation")
        
        # Prepare broker config
        broker_config = broker_config_from_session(session_doc)
        broker_config["free_cash"] = session_doc.get("free_cash")
        
        # Get MongoDB connection details
        mongo_uri = os.environ.get(
            "MONGO_URI",
            "mongodb+srv://mintzy01ai_db_user:zTqQRkovgKbLXQdp@cluster0.cztcxpr.mongodb.net/?appName=Cluster0"
        )
        mongo_db_name = os.environ.get("MONGO_DB_NAME", "mintzy_plugin")
        mongo_config_db_name = os.environ.get("MONGO_CONFIG_DB_NAME") or "test"
        
        # Create stop event and health queue for this worker
        cls._ensure_manager()
        stop_event = mp.Event()
        
        # Create worker process
        process = Process(
            target=_trader_worker,
            args=(
                session_id,
                strategy,
                symbols,
                allocations,
                time_frame,
                candle,
                broker_config,
                trading_logs_collection.name,
                stop_event,
                cls._health_queue,
                mongo_uri,
                mongo_db_name,
                configuration_id,
                mongo_config_db_name,
                leverage_multiplier,
                simulation_logs,
            ),
            daemon=False  # Not daemon - we want proper cleanup
        )
        
        # Start process
        process.start()

        try:
            pid_key = f"{cls.REDIS_KEY_PREFIX}{session_id}"
            meta_key = cls._session_meta_key(session_id)
            meta_payload = json.dumps({
                "strategy": strategy,
                "pid": process.pid,
                "started_at": time.time(),
            })
            redis_client = cls._redis()
            if redis_client:
                pipe = redis_client.pipeline()
                pipe.setex(pid_key, cls.SESSION_REDIS_TTL, str(process.pid))
                pipe.setex(meta_key, cls.SESSION_REDIS_TTL, meta_payload)
                pipe.execute()
                print(
                    f"[SessionManager] Redis save ok session={session_id} "
                    f"pid={process.pid} strategy={strategy}"
                )
        except Exception as e:
            print(f"[SessionManager] Redis save failed: {e}")

        cls._save_worker_pid(session_id, process.pid)
        cls._save_worker_pid_file(session_id, process.pid)
            
        # Register worker
        worker = WorkerProcess(session_id, process, stop_event, cls._health_queue)
        cls._workers[session_id] = worker
        
        print(f"[SessionManager] Session {session_id} started in process {process.pid}")
        
        return {
            "session_id": session_id,
            "pid": process.pid,
            "started_at": worker.started_at
        }
    
    # @classmethod
    # def stop_session(cls, session_id: str):
    #     """Stop a trading session gracefully"""
        
    #     worker = cls._workers.get(session_id)
        
    #     if not worker:
    #         print(f"[SessionManager] No active session {session_id}")
    #         return False
        
    #     print(f"[SessionManager] Stopping session {session_id} (PID: {worker.process.pid})")
        
    #     # Signal worker to stop
    #     worker.stop_event.set()
        
    #     # Wait for graceful shutdown
    #     worker.process.join(timeout=15)
        
    #     # Force cleanup if still alive
    #     if worker.is_alive():
    #         print(f"[SessionManager] Force terminating session {session_id}")
    #         worker.process.terminate()
    #         worker.process.join(timeout=3)
            
    #         if worker.is_alive():
    #             worker.process.kill()
        
    #     # Remove from registry
    #     cls._workers.pop(session_id, None)
        
    #     print(f"[SessionManager] Session {session_id} stopped")
    #     return True


    @classmethod
    def stop_session_signal_only(cls, session_id: str) -> bool:
        """
        Signal the trading worker to stop without blocking for process exit.
        Safe to call from any gunicorn worker (local mp.Event or cross-worker SIGTERM).
        """
        print(f"[SessionManager] stop_session_signal_only: '{session_id}'")
        worker = cls._workers.get(session_id)

        if worker:
            print(f"[SessionManager] Signal-only local worker for {session_id}")
            worker.stop_event.set()
            return True

        try:
            pid_str = cls._redis().get(f"{cls.REDIS_KEY_PREFIX}{session_id}")
            if not pid_str:
                print(
                    f"[SessionManager] Signal-only: no local worker or Redis PID for {session_id} "
                    "— worker may already be stopped"
                )
                return True

            pid = int(pid_str)
            print(f"[SessionManager] Signal-only SIGTERM PID={pid} for {session_id}")
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                print(f"[SessionManager] Signal-only: PID={pid} already exited")
            return True
        except Exception as e:
            print(f"[SessionManager] Signal-only stop failed for {session_id}: {e}")
            return False

    @classmethod
    def stop_session(cls, session_id: str):
        print(f"[SessionManager] Stop request aaya: '{session_id}'")
        print(f"[SessionManager] Current workers: {list(cls._workers.keys())}")

        cls._reconcile_session_registry(session_id)
        worker = cls._workers.get(session_id)

        if not worker:
            print(f"[SessionManager] Local memory mein nahi mila — PID lookup kar raha hoon...")
            pid = cls._resolve_worker_pid(session_id)

            if not pid:
                print(f"[SessionManager] No worker PID found - nothing stopped")
                cls._clear_worker_pid(session_id)
                cls._clear_worker_pid_file(session_id)
                return False

            print(f"[SessionManager] PID={pid} mila — terminate kar raha hoon...")
            stopped = cls._terminate_pid(pid)
            if stopped and cls._pid_alive(pid):
                stopped = cls._wait_for_pid_exit(pid, timeout=90)
            if not stopped:
                print(f"[SessionManager] PID={pid} did not stop cleanly for {session_id}")
                return False

            redis_client = cls._redis()
            if redis_client:
                cls._delete_session_redis_keys(session_id)

            cls._clear_worker_pid(session_id)
            cls._clear_worker_pid_file(session_id)
            print(f"[SessionManager] Session {session_id} stopped via PID lookup")
            return stopped

        print(f"[SessionManager] Local memory mein mila — normal shutdown...")

        worker.stop_event.set()
        worker.process.join(timeout=60)

        if worker.is_alive():
            print(f"[SessionManager] Force terminating {session_id}")
            worker.process.terminate()
            worker.process.join(timeout=5)
            if worker.is_alive():
                worker.process.kill()
                worker.process.join(timeout=5)

        if worker.is_alive():
            print(f"[SessionManager] Worker still alive after stop attempt: {session_id}")
            return False

        cls._delete_session_redis_keys(session_id)
        cls._workers.pop(session_id, None)
        cls._clear_worker_pid(session_id)
        cls._clear_worker_pid_file(session_id)
        print(f"[SessionManager] Session {session_id} stopped")
        return True

    @classmethod
    def stop_simulation_session_async(cls, session_id: str) -> bool:
        """
        Non-blocking simulation stop: set flags, signal worker, return immediately.
        Worker completes pyramid/shutdown in background and updates Redis stop job.
        """
        print(f"[SessionManager] Async simulation stop request: '{session_id}'")
        existing = cls.read_stop_job(session_id)
        if existing and existing.get("status") in ("stopping", "completed"):
            print(
                f"[SessionManager] Stop job already {existing.get('status')} for {session_id} "
                "— idempotent accept"
            )
            return True

        if not cls._mark_simulation_stop(session_id):
            print(
                f"[SessionManager] Simulation stop aborted for {session_id} "
                "— Redis simulation-stop flag was not set"
            )
            return False

        cls.init_stop_job(session_id, status="stopping", phase="signal_sent")

        if not cls.stop_session_signal_only(session_id):
            cls.fail_stop_job(session_id, "Failed to signal simulation worker")
            return False

        cls.update_stop_job(session_id, phase="waiting_worker")
        return True

    @classmethod
    def stop_simulation_session(cls, session_id: str):
        """
        Blocking simulation stop (legacy / ?wait=true).
        Sets a Redis flag so the worker exit handler keeps status=authenticated in Mongo.
        """
        print(f"[SessionManager] Simulation stop request: '{session_id}'")
        if not cls._mark_simulation_stop(session_id):
            print(
                f"[SessionManager] Simulation stop aborted for {session_id} "
                "— Redis simulation-stop flag was not set"
            )
            return False
        cls.init_stop_job(session_id, status="stopping", phase="blocking_stop")
        stopped = cls.stop_session(session_id)
        cls._cleanup_worker(session_id)
        if stopped:
            fully_stopped = cls.ensure_worker_stopped(session_id, timeout=90)
            if not fully_stopped:
                print(f"[SessionManager] Simulation worker for {session_id} did not exit in time")
                cls.fail_stop_job(session_id, "Worker did not exit in time")
                return False
        if stopped:
            handoff = cls.read_pyramid_handoff_result(session_id) or dict(_PYRAMID_HANDOFF_BLOCKED)
            cls.complete_stop_job(session_id, handoff, simulation_stop=True)
        return stopped
    
    @classmethod
    def exit_symbol_for_session(cls, session_id: str, symbol: str) -> dict:
        """
        Send a single-symbol exit request to the trader worker process via Redis queue.
        Works across gunicorn workers because Redis is shared.
        """
        print(f"[SessionManager] Exit request: session={session_id} symbol={symbol}")
        
        # Verify session exists (local or Redis)
        worker = cls._workers.get(session_id)
        if not worker:
            # Check Redis for PID (cross-worker case)
            try:
                pid_str = cls._redis().get(f"{cls.REDIS_KEY_PREFIX}{session_id}")
                if not pid_str:
                    return {
                        "success": False,
                        "symbol": symbol,
                        "message": f"Session {session_id} not found (not running)"
                    }
            except Exception as e:
                return {
                    "success": False,
                    "symbol": symbol,
                    "message": f"Redis error checking session: {e}"
                }
        
        # Push exit request to Redis queue
        try:
            exit_queue_key = f"autotrader:exit_request:{session_id}"
            payload = json.dumps({"symbol": symbol.upper()})
            cls._redis().rpush(exit_queue_key, payload)
            # Set TTL on the queue key so it auto-cleans (5 minutes)
            cls._redis().expire(exit_queue_key, 300)
            
            print(f"[SessionManager] Ã¢Å“â€¦ Exit request pushed to Redis for {symbol}")
            
            # Wait briefly for the result (max 10 seconds)
            result_key = f"autotrader:exit_result:{session_id}:{symbol.upper()}"
            for _ in range(20):  # 20 Ãƒâ€” 0.5s = 10s
                time.sleep(0.5)
                result_raw = cls._redis().get(result_key)
                if result_raw:
                    cls._redis().delete(result_key)  # cleanup
                    return json.loads(result_raw)
            
            # Timeout Ã¢â‚¬â€ request was sent but no result yet
            return {
                "success": True,
                "symbol": symbol,
                "message": f"Exit request sent for {symbol}. Order is being processed (reconciliation will handle it)."
            }
            
        except Exception as e:
            return {
                "success": False,
                "symbol": symbol,
                "message": f"Failed to send exit request: {e}"
            }
    

  
    @classmethod
    def get_session_status(cls, session_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a trading session (local process or Redis/Mongo PID)."""
        
        worker = cls._workers.get(session_id)
        
        if worker:
            return {
                "session_id": session_id,
                "pid": worker.process.pid,
                "is_alive": worker.is_alive(),
                "is_healthy": worker.is_healthy(),
                "started_at": worker.started_at,
                "uptime": time.time() - worker.started_at,
                "last_heartbeat": worker.last_heartbeat
            }

        pid = cls._resolve_worker_pid(session_id)
        if not pid:
            return None
        alive = cls._pid_alive(pid)
        return {
            "session_id": session_id,
            "pid": pid,
            "is_alive": alive,
            "is_healthy": alive,
            "started_at": None,
            "uptime": None,
            "last_heartbeat": None,
        }
    
    @classmethod
    def list_sessions(cls) -> Dict[str, Dict[str, Any]]:
        """List all active sessions"""
        return {
            session_id: cls.get_session_status(session_id)
            for session_id in cls._workers.keys()
        }
    
    @classmethod
    def stop_all_sessions(cls):
        """Stop all trading sessions"""
        
        print("[SessionManager] Stopping all sessions...")
        
        session_ids = list(cls._workers.keys())
        
        for session_id in session_ids:
            cls.stop_session(session_id)
        
        # Stop monitor
        cls._monitor_stop.set()
        if cls._monitor_thread:
            cls._monitor_thread.join(timeout=5)
        
        print("[SessionManager] All sessions stopped")

SessionManager.LiveStartPrepResult = LiveStartPrepResult
