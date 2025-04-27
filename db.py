# data/db.py
import logging
import os
import asyncio
from typing import Optional, Dict, Any, List, Union

# Import required libraries (ensure they are installed)
import asyncpg
import redis.asyncio as aioredis
from supabase import create_client, Client as SupabaseSyncClient

# Import for settings dependency
from config.settings import Settings # Assume Settings class is in config.settings

# Supabase/PostgREST error imports (match actual exception types if possible)
try:
     from postgrest.exceptions import APIError as PostgrestAPIError
except ImportError:
     PostgrestAPIError = Exception # Fallback

logger = logging.getLogger(__name__)

class Database:
    """
    Asynchronous database connection manager using connection pooling.
    Initializes Supabase sync client and async pools for PostgreSQL and Redis.
    Provides methods for accessing clients/pools and executing raw PG queries.
    """

    def __init__(self, settings: Settings):
        """
        Initialize database connection manager.
        Pools are initialized asynchronously later via `connect`.
        Supabase sync client is created here.

        Args:
            settings: The application's configuration settings instance.
        """
        self.settings = settings
        self._supabase_sync_client_instance: Optional[SupabaseSyncClient] = None
        self._redis_connection_pool: Optional[aioredis.ConnectionPool] = None
        self._postgres_pool: Optional[asyncpg.Pool] = None
        self.use_local_db = os.environ.get("USE_LOCAL_DB", "false").lower() == "true"

        # --- Initialize Supabase Sync Client ---
        self._initialize_supabase_sync_client()

    def _initialize_supabase_sync_client(self):
        """Handles synchronous initialization of the Supabase client."""
        if self.use_local_db:
            logger.info("USING MOCK SUPABASE CLIENT: REST API calls are mocked.")
            # --- Using improved mock structure ---
            class MockSupabaseResponse:
                def __init__(self, data: List[Any] = [], count: Optional[int] = 0, error: Any = None, status_code: int = 200):
                    self.data = data
                    self.count = count
                    self.error = error
                    self.status_code = status_code # Mimic httpx structure somewhat

            class MockSupabaseQueryBuilder:
                 def __init__(self, name: str = "mock_table"): self.name = name; self._error = None
                 def table(self, name: str): logger.debug(f"MOCK Supabase: table({name})"); self.name=name; return self
                 def select(self, *a, **kw): logger.debug(f"MOCK Supabase: select(...) on {self.name}"); return self
                 def insert(self, *a, **kw): logger.debug(f"MOCK Supabase: insert(...) into {self.name}"); return self
                 def update(self, *a, **kw): logger.debug(f"MOCK Supabase: update(...) on {self.name}"); return self
                 def delete(self, *a, **kw): logger.debug(f"MOCK Supabase: delete(...) from {self.name}"); return self
                 def eq(self, col, val): logger.debug(f"MOCK Supabase: eq({col}, {val}) on {self.name}"); return self
                 def neq(self, col, val): logger.debug(f"MOCK Supabase: neq({col}, {val})"); return self
                 def in_(self, col, vals): logger.debug(f"MOCK Supabase: in_({col}, ...)"); return self
                 def maybe_single(self): logger.debug("MOCK Supabase: maybe_single()"); return self
                 def limit(self, *a, **kw): logger.debug("MOCK Supabase: limit(...)"); return self
                 def order(self, *a, **kw): logger.debug("MOCK Supabase: order(...)"); return self
                 def execute(self, *a, **kw) -> MockSupabaseResponse:
                     logger.debug(f"MOCK Supabase: execute() on {self.name}")
                     # Return success with empty data by default
                     return MockSupabaseResponse()

            self._supabase_sync_client_instance = MockSupabaseQueryBuilder() # Assign mock
        else:
             if not self.settings.supabase_url or not self.settings.supabase_service_key:
                  raise ValueError("Supabase URL and Service Key are required.")
             try:
                  logger.info(f"Initializing Supabase sync client (URL: {self.settings.supabase_url[:25]}...)")
                  # Note: create_client can raise if URL/Key are invalid format at init time
                  self._supabase_sync_client_instance = create_client(
                      self.settings.supabase_url,
                      self.settings.supabase_service_key # MUST BE SERVICE ROLE KEY
                  )
                  logger.info("Supabase sync client initialized.")
             except Exception as e:
                  logger.critical("CRITICAL: Failed to initialize Supabase sync client!", exc_info=True)
                  raise RuntimeError(f"Supabase client init failed: {e}") from e

    # --- Public Properties/Getters ---
    @property
    def supabase_client(self) -> SupabaseSyncClient:
        """Returns the initialized synchronous Supabase client instance."""
        if not self._supabase_sync_client_instance:
            raise RuntimeError("Supabase sync client accessed before initialization.")
        return self._supabase_sync_client_instance

    @property
    def redis_pool(self) -> Optional[aioredis.ConnectionPool]:
        """Returns the initialized Redis connection pool (or None if not connected)."""
        # Optional: Raise error if not connected? Or allow lazy connection attempt here?
        # if not self._redis_connection_pool:
        #      raise RuntimeError("Redis pool accessed before calling db.connect()")
        return self._redis_connection_pool

    @property
    def pg_pool(self) -> Optional[asyncpg.Pool]:
        """Returns the initialized asyncpg Pool (or None if not connected)."""
        return self._postgres_pool

    # --- Async Connection Pool Initialization ---
    async def connect(self):
        """Establishes asynchronous connection pools for Postgres and Redis."""
        if self.use_local_db:
            await asyncio.gather(
                 self._connect_local_pg_pool(),
                 self._connect_local_redis_pool(),
                 return_exceptions=True # Log errors but don't necessarily stop other pool
            )
        else:
            # Connect to external services concurrently
             results = await asyncio.gather(
                 self._connect_supabase_pg_pool(),
                 self._connect_redis_pool(),
                 return_exceptions=True
             )
             # Check results for errors
             for i, result in enumerate(results):
                  if isinstance(result, Exception):
                       service = "PostgreSQL" if i == 0 else "Redis"
                       logger.critical(f"Failed to connect {service} pool during startup: {result}")
                       # Consider raising an error here to prevent app startup if a pool fails?
                       # raise ConnectionError(f"Failed to connect {service} pool") from result

        if self._postgres_pool and self._redis_connection_pool:
            logger.info("Database connection pools established successfully.")
        elif self._postgres_pool or self._redis_connection_pool:
            logger.warning("One or more database connection pools failed to initialize.")
        else:
            # Consider this fatal? Or allow app to run degraded?
             logger.critical("CRITICAL: Both PostgreSQL and Redis connection pools failed to initialize.")
             # raise ConnectionError("Failed to establish critical database connections.")


    # --- Internal Pool Connection Logic ---
    async def _connect_redis_pool(self):
        """Establishes Redis connection pool."""
        if self._redis_connection_pool: return # Already connected
        if not self.settings.redis_host: logger.warning("Redis host not configured, skipping Redis pool."); return

        logger.info(f"Connecting Redis Pool: {self.settings.redis_host}:{self.settings.redis_port} SSL={self.settings.redis_ssl}")
        redis_url = f"redis{'s' if self.settings.redis_ssl else ''}://{':' + self.settings.redis_password + '@' if self.settings.redis_password else ''}{self.settings.redis_host}:{self.settings.redis_port}/0"
        try:
            # Create pool, consider adding max_connections etc.
            pool = aioredis.ConnectionPool.from_url(redis_url, max_connections=20, decode_responses=True)
            # Test with a connection from the pool
            redis_conn = aioredis.Redis(connection_pool=pool)
            if await redis_conn.ping():
                 logger.info("Redis connection pool ping successful.")
                 self._redis_connection_pool = pool
            else:
                 raise ConnectionError("Redis ping failed after pool creation.")
            await redis_conn.close() # Release test connection explicitly? Check library details.
        except Exception as e:
            logger.critical(f"Failed to establish Redis connection pool: {e}", exc_info=False) # Don't need full trace always
            self._redis_connection_pool = None
            raise # Re-raise to be caught by asyncio.gather in connect()

    async def _connect_supabase_pg_pool(self):
        """Establishes PostgreSQL direct connection pool."""
        if self._postgres_pool: return
        if not self.settings.db_host or not self.settings.db_password:
            logger.critical("Missing DB_HOST or DB_PASSWORD for PostgreSQL pool.")
            raise ValueError("DB host and password required.")

        logger.info(f"Connecting PostgreSQL Pool: {self.settings.db_host}:{self.settings.db_port} DB={self.settings.db_name} User={self.settings.db_user}")
        try:
            # Use SSL context for secure connection to Supabase
            # ssl_context = ssl.create_default_context(cafile=certifi.where()) # Example using certifi
            # ssl_context.check_hostname = False # Maybe needed depending on Supabase setup
            # ssl_context.verify_mode = ssl.CERT_NONE # Less secure if used

            pool = await asyncpg.create_pool(
                user=self.settings.db_user,
                password=self.settings.db_password,
                host=self.settings.db_host,
                port=self.settings.db_port,
                database=self.settings.db_name,
                ssl='require', # Force SSL - use SSLContext object if specific cert needed
                min_size=2, max_size=10,
                # Set statement timeout globally for the pool? (e.g., 30s)
                server_settings={'statement_timeout': '30000'}
            )
            if pool:
                 # Test connection
                 async with pool.acquire() as conn: await conn.fetchval("SELECT 1")
                 logger.info("PostgreSQL connection pool established successfully.")
                 self._postgres_pool = pool
            else:
                 raise ConnectionError("asyncpg.create_pool returned None")
        except Exception as e:
            logger.critical(f"Failed to establish PostgreSQL connection pool: {e}", exc_info=False)
            self._postgres_pool = None
            raise

    # --- Mock/Local pool initializers (mostly unchanged) ---
    async def _connect_local_pg_pool(self): #...
         # ... (try/except as before) ...
    async def _connect_local_redis_pool(self): # ...
        # ... (try/except as before) ...

    # --- Async DB Query Methods ---
    async def fetch_all(self, query: str, *params) -> List[asyncpg.Record]:
        """Executes a query and fetches all results using pool."""
        if not self._postgres_pool: raise ConnectionError("PostgreSQL pool unavailable.")
        logger.debug(f"Executing fetch_all: {query} | PARAMS: {params}")
        try:
            # Acquire connection temporarily from the pool
            async with self._postgres_pool.acquire() as conn:
                return await conn.fetch(query, *params)
        except Exception as e:
             logger.exception(f"Error in fetch_all query: {query[:100]}...", exc_info=True)
             raise # Re-raise to allow caller to handle

    async def fetch_one(self, query: str, *params) -> Optional[asyncpg.Record]:
        """Executes a query and fetches one result or None using pool."""
        if not self._postgres_pool: raise ConnectionError("PostgreSQL pool unavailable.")
        logger.debug(f"Executing fetch_one: {query} | PARAMS: {params}")
        try:
             async with self._postgres_pool.acquire() as conn:
                return await conn.fetchrow(query, *params)
        except Exception as e:
             logger.exception(f"Error in fetch_one query: {query[:100]}...", exc_info=True)
             raise

    async def execute_commit(self, query: str, *params) -> str:
        """Executes a query modifying data (INSERT/UPDATE/DELETE) using pool."""
        if not self._postgres_pool: raise ConnectionError("PostgreSQL pool unavailable.")
        logger.debug(f"Executing execute_commit: {query} | PARAMS: {params}")
        try:
             async with self._postgres_pool.acquire() as conn:
                  # Consider using transaction if multiple execute calls needed atomically
                  # async with conn.transaction():
                  #     status = await conn.execute(query, *params)
                  status = await conn.execute(query, *params)
                  return status # e.g., "INSERT 0 1"
        except Exception as e:
             logger.exception(f"Error executing execute_commit query: {query[:100]}...", exc_info=True)
             raise


    # --- Shutdown Method ---
    async def close(self):
        """Gracefully closes PostgreSQL and Redis connection pools."""
        logger.info("Attempting to close database connection pools...")
        closed_pg = False
        closed_redis = False
        if self._postgres_pool:
            logger.debug("Closing PostgreSQL pool...")
            try:
                 await asyncio.wait_for(self._postgres_pool.close(), timeout=5.0)
                 logger.info("PostgreSQL connection pool closed.")
                 closed_pg = True
            except asyncio.TimeoutError:
                  logger.error("Timeout closing PostgreSQL pool.")
            except Exception as e:
                 logger.exception("Error closing PostgreSQL pool", exc_info=True)
            finally: self._postgres_pool = None

        if self._redis_connection_pool:
            logger.debug("Disconnecting Redis pool...")
            try:
                 # Use disconnect() for ConnectionPool
                 await asyncio.wait_for(self.redis_pool.disconnect(inuse_connections=True), timeout=5.0)
                 logger.info("Redis connection pool disconnected.")
                 closed_redis = True
            except asyncio.TimeoutError:
                  logger.error("Timeout disconnecting Redis pool.")
            except Exception as e:
                 logger.exception("Error disconnecting Redis pool", exc_info=True)
            finally: self._redis_connection_pool = None

        # Sync client has no explicit close in docs? Garbage collection handles it.
        self._supabase_sync_client_instance = None

        if closed_pg and closed_redis: logger.info("All database pools closed/disconnected.")
        elif closed_pg or closed_redis: logger.warning("Only some database pools closed.")
        else: logger.error("Failed to close database pools cleanly.")