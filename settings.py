# config/settings.py
import os
import logging
from typing import Optional, Dict, Any

# Use pydantic-settings for V2 style env var loading & validation
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, ValidationError, PostgresDsn, AnyHttpUrl, field_validator

logger = logging.getLogger(__name__)

# Valid log levels recognised by Python's logging module
VALID_LOG_LEVELS = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables or .env file using pydantic-settings.
    Defines required API keys, database connections, and operational parameters.
    """

    # --- API Keys (Required - MUST be set in .env or environment) ---
    helius_api_key: str = Field(..., description="API Key for Helius RPC/WSS/DAS")
    solscan_api_key: str = Field(..., description="API Key for Solscan Pro v2")
    dune_api_key: str = Field(..., description="API Key for Dune Analytics API v1/v2")
    telegram_bot_token: str = Field(..., description="Telegram Bot API Token from BotFather")

    # --- Supabase (Required) ---
    supabase_url: AnyHttpUrl = Field(..., description="Supabase Project URL (e.g., https://<ref>.supabase.co)")
    supabase_service_key: str = Field(..., description="Supabase Service Role Key (SECRET!) - Grants admin privileges")

    # --- Direct Database Connection (Required - point to Supabase DB) ---
    db_user: str = Field(default="postgres", description="Direct PostgreSQL database user")
    db_password: str = Field(..., description="Direct PostgreSQL database password (SECRET!)") # Required!
    db_host: str = Field(..., description="Direct PostgreSQL database host (e.g., db.<ref>.supabase.co)") # Required!
    db_port: int = Field(default=5432, description="Direct PostgreSQL database port (usually 5432)")
    db_name: str = Field(default="postgres", description="Direct PostgreSQL database name")

    # --- Redis (Optional with defaults) ---
    redis_host: str = Field(default="localhost", description="Redis server host")
    redis_port: int = Field(default=6379, description="Redis server port")
    redis_password: Optional[str] = Field(default=None, description="Redis password (if required)")
    redis_ssl: bool = Field(default=False, description="Use SSL/TLS for Redis connection")

    # --- Optional API Server (for webhooks or frontend if implemented) ---
    api_host: str = Field(default="0.0.0.0", description="Host for the optional API server")
    api_port: int = Field(default=8080, description="Port for the optional API server")

    # --- Bot/App Tuning Parameters ---
    log_level: str = Field(default="INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR)")
    classification_cache_ttl_seconds: int = Field(default=21600, gt=0, description="TTL for cached classifications (seconds)") # Add >0 constraint
    processed_tx_ttl_seconds: int = Field(default=86400, gt=0, description="TTL for processed transaction IDs (seconds)") # Add >0 constraint
    monitoring_interval_seconds: int = Field(default=300, gt=0, description="Interval for periodic background tasks (seconds)") # Add >0 constraint
    notification_send_delay: float = Field(default=0.15, ge=0, description="Delay between sending queued Telegram messages (seconds)") # Add >=0 constraint
    websocket_reconnect_delay: int = Field(default=5, gt=0, description="Delay before attempting WebSocket reconnect (seconds)") # Add >0 constraint

    # --- Pydantic-Settings Configuration ---
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore'
    )

    # --- Custom Validators ---
    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        """Ensure log_level is a valid Python logging level name."""
        level = value.upper()
        if level not in VALID_LOG_LEVELS:
            raise ValueError(f"Invalid LOG_LEVEL '{value}'. Must be one of: {', '.join(VALID_LOG_LEVELS)}")
        return level

    # --- Derived Properties ---
    @property
    def database_connection_string(self) -> str:
        """Constructs a standard PostgreSQL DSN connection string if components are set."""
        # More robust check before building
        if not self.db_host or not self.db_password:
             logger.warning("DB host or password missing, cannot generate full DSN.")
             return f"postgresql://{self.db_user}@/" # Minimal invalid DSN
        # Using Pydantic's helper is safer if compatible with installed Pydantic/Settings version
        try:
            return str(PostgresDsn.build(
                 scheme="postgresql",
                 username=self.db_user,
                 password=self.db_password,
                 host=self.db_host,
                 port=self.db_port,
                 path=f"{self.db_name or ''}", # Handle potential empty db_name if allowed
             ))
        except (ValidationError, ImportError): # Catch potential issues building DSN
            logger.warning("Falling back to manual DSN string construction (PostgresDsn unavailable/failed).")
            return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

# --- Global Singleton Instance & Enhanced Validation ---
try:
    settings = Settings()
    # Added check specifically for required fields
    required_fields_for_operation = [
        'helius_api_key', 'solscan_api_key', 'dune_api_key',
        'telegram_bot_token', 'supabase_url', 'supabase_service_key',
        'db_password', 'db_host' # Explicitly check these now
    ]
    missing = [field for field in required_fields_for_operation if not getattr(settings, field, None)]
    if missing:
         raise ValueError(f"Missing critical configuration settings: {', '.join(missing)}")

    logger.info("Configuration loaded and validated successfully.")
    logger.debug(f"Loaded Settings: DB Host={settings.db_host}:{settings.db_port}, Redis={settings.redis_host}:{settings.redis_port}")

except ValidationError as e:
     logger.critical(f"CRITICAL CONFIGURATION ERROR (Pydantic): {e}", exc_info=False) # Don't need full trace for config validation
     exit(1)
except ValueError as e: # Catch our explicit missing keys error
     logger.critical(f"CRITICAL CONFIGURATION ERROR: {e}")
     exit(1)
except Exception as e: # Catch any other init errors
     logger.critical(f"CRITICAL UNEXPECTED ERROR loading settings: {e}", exc_info=True)
     exit(1)


# --- Import elsewhere ---
# from config.settings import settings