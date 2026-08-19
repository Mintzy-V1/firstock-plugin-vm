
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
)
import signal





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
    mongo_db_name: str
):
    """
    Worker process that runs a single AutoTrader instance.
    Isolated from other traders - has its own Python interpreter and memory space.
    """
    import signal
    def handle_sigterm(signum, frame):
        print(f"[Worker-{session_id}] SIGTERM mila Ã¢â‚¬â€ graceful shutdown...")
        stop_event.set()   # Ã¢â€ Â stop_event set kar do Ã¢â‚¬â€ baaki sab automatically hoga
    
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
        
        # Select trader class
        TRADER_MAP = {"A": AutoTraderA, "B": AutoTraderB}
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
        
        # Configure trader
        trader.session_id = session_id
        trader.session = restored
        trader.ui_session_id = session_id
        trader.symbol_allocations = {k: v["capital"] for k, v in allocations.items()}
        trader.initial_allocations = allocations
        
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
            kwargs={"initial_allocations": allocations},
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
            ltp_stream = create_ltp_stream(broker, trader.on_ltp_tick)
            ltp_stream.start(list(allocations.keys()))
            print(f"[Worker-{session_id}] LiveLTPStream started for {len(allocations)} symbols")
        except Exception as e:
            print(f"[Worker-{session_id}] LiveLTPStream failed to start: {e}")

        # Monitor for stop signal AND single-symbol exit requests
        exit_queue_key = f"autotrader:exit_request:{session_id}"
        while trader_thread.is_alive() and not stop_event.is_set():
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
        
        # If stop was requested, shutdown trader
        if stop_event.is_set():
            print(f"[Worker-{session_id}] Stop requested, shutting down...")
            trader.shutdown() # but inside the shutdown we have not write the logic of exiting the orders and all and in the while loop we have not checked the stop_event flag
            print("shutdown called successfully")
            trader_thread.join(timeout=60)

        if ltp_stream is not None:
            try:
                ltp_stream.stop()
            except Exception as e:
                print(f"[Worker-{session_id}] LiveLTPStream stop error: {e}")


        
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
    PID_DIR = os.environ.get("AUTOTRADER_PID_DIR", "/tmp")


    
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

            import psutil

            try:
                proc = psutil.Process(pid)
                proc.wait(timeout=60)
                print(f"[SessionManager] Process {pid} exited gracefully")
            except psutil.TimeoutExpired:
                print(f"[SessionManager] PID={pid} still alive after 60s, sending SIGKILL")
                os.kill(pid, signal.SIGKILL)
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
        
        # Check if session already exists
        if session_id in cls._workers:
            print(f"[SessionManager] Session already running: {session_id}")
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
        
        # Prepare broker config
        broker_config = broker_config_from_session(session_doc)
        
        # Get MongoDB connection details
        mongo_uri = os.environ.get(
            "MONGO_URI",
            "mongodb+srv://ankitarrow:ankitarrow@cluster0.zcajdur.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        )
        mongo_db_name = os.environ.get("MONGO_DB_NAME", "mintzy_plugin")
        
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
                mongo_db_name
            ),
            daemon=False  # Not daemon - we want proper cleanup
        )
        
        # Start process
        process.start()

        try:
            redis_client = cls._redis()
            if redis_client:
                redis_client.setex(
                    f"{cls.REDIS_KEY_PREFIX}{session_id}",
                    86400,   # 24 hour TTL
                    str(process.pid)
                )
                print(f"[SessionManager] Redis save ok session={session_id} pid={process.pid}")
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
    def stop_session(cls, session_id: str):
        print(f"[SessionManager] Stop request aaya: '{session_id}'")
        print(f"[SessionManager] Current workers: {list(cls._workers.keys())}")

        worker = cls._workers.get(session_id)

        if not worker:
            print(f"[SessionManager] Local memory mein nahi mila — PID lookup kar raha hoon...")
            pid = cls._resolve_worker_pid(session_id)

            if not pid:
                print(f"[SessionManager] No worker PID found — treating as already stopped")
                cls._clear_worker_pid(session_id)
                cls._clear_worker_pid_file(session_id)
                return True

            print(f"[SessionManager] PID={pid} mila — terminate kar raha hoon...")
            stopped = cls._terminate_pid(pid)

            redis_client = cls._redis()
            if redis_client:
                try:
                    redis_client.delete(f"{cls.REDIS_KEY_PREFIX}{session_id}")
                except Exception as e:
                    print(f"[SessionManager] Redis delete failed: {e}")

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

        redis_client = cls._redis()
        if redis_client:
            try:
                redis_client.delete(f"{cls.REDIS_KEY_PREFIX}{session_id}")
            except Exception as e:
                print(f"[SessionManager] Redis delete failed: {e}")

        cls._workers.pop(session_id, None)
        cls._clear_worker_pid(session_id)
        cls._clear_worker_pid_file(session_id)
        print(f"[SessionManager] Session {session_id} stopped")
        return True
    
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
        """Get status of a trading session"""
        
        worker = cls._workers.get(session_id)
        
        if not worker:
            return None
        
        return {
            "session_id": session_id,
            "pid": worker.process.pid,
            "is_alive": worker.is_alive(),
            "is_healthy": worker.is_healthy(),
            "started_at": worker.started_at,
            "uptime": time.time() - worker.started_at,
            "last_heartbeat": worker.last_heartbeat
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
