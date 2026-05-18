from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    environment: str = "development"

    binance_ws_testnet_url: str
    binance_ws_mainnet_url: str = "wss://stream.binance.com:9443/ws"
    binance_api_key: str
    binance_secret_key: str

    database_url: str
    db_pool_min_size: int = 1
    db_pool_max_size: int = 3

    redis_url: str
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    log_level: str = "INFO"

    ws_symbol: str = "BTCUSDT"
    ws_reconnect_max_delay: float = 60.0
    use_mainnet: bool = False

    @property
    def active_ws_url(self) -> str:
        return (
            self.binance_ws_mainnet_url
            if self.use_mainnet
            else self.binance_ws_testnet_url
        )

    @property
    def is_development(self) -> bool:
        return self.environment == "development"

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


class DevelopmentSettings(Settings):
    log_level: str = "DEBUG"
    db_pool_min_size: int = 1
    db_pool_max_size: int = 3


class ProductionSettings(Settings):
    log_level: str = "INFO"
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    import os
    env = os.getenv("ENVIRONMENT", "development").lower()
    if env == "production":
        return ProductionSettings()
    return DevelopmentSettings()


settings = get_settings()
