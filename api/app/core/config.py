from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database
    database_url: str = "postgresql+asyncpg://orderuser:orderpass@localhost:5432/ordersdb"

    # App
    environment: str = "development"
    log_level: str = "INFO"
    app_name: str = "outbox-cdc-api"
    app_version: str = "1.0.0"

    # Pagination
    default_page_size: int = 20
    max_page_size: int = 100

    @property
    def is_development(self) -> bool:
        return self.environment == "development"


@lru_cache
def get_settings() -> Settings:
    return Settings()