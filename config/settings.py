from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Environment identity
    environment: str = "development"

    # Binance
    binance_ws_testnet_url: str
    binance_api_key: str
    binance_secret_key: str

    # Database
    database_url: str
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10

    # Redis
    redis_url: str

    # Logging
    log_level: str = "INFO"

    # Stream
    ws_symbol: str = "BTCUSDT"
    ws_reconnect_max_delay: float = 60.0

    @property
    def is_development(self) -> bool:
        return self.environment == "development"

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


class DevelopmentSettings(Settings):
    """
    Development overrides — more verbose, smaller pools,
    tolerant of missing optional config.
    """
    log_level: str = "DEBUG"
    db_pool_min_size: int = 1
    db_pool_max_size: int = 3


class ProductionSettings(Settings):
    """
    Production overrides — structured JSON logs,
    larger pools, stricter behaviour.
    """
    log_level: str = "INFO"
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Returns the correct Settings instance for the current environment.
    Cached — instantiated once per process.
    """
    import os
    env = os.getenv("ENVIRONMENT", "development").lower()

    if env == "production":
        return ProductionSettings()
    return DevelopmentSettings()


# Module-level singleton — import this everywhere
settings = get_settings()
