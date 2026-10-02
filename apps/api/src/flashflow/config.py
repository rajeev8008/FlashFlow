from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://flashflow:flashflow@postgres:5432/flashflow"
    redis_url: str = "redis://redis:6379/0"
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_inventory_topic: str = "inventory-events"
    kafka_inventory_partitions: int = Field(6, ge=1)
    kafka_consumer_group: str = "flashflow-inventory"
    kafka_dlq_topic: str = "inventory-events-dlq"
    consumer_max_attempts: int = Field(3, ge=1)
    consumer_retry_seconds: float = Field(0.5, ge=0)
    websocket_heartbeat_seconds: float = Field(15, gt=0)
    app_env: Literal["development", "test", "production"] = "production"
    enable_chaos: bool = False
    chaos_token: str = ""
    breaker_failure_threshold: int = Field(3, ge=1)
    breaker_recovery_seconds: float = Field(5, gt=0)
    breaker_half_open_trials: int = Field(1, ge=1)
    inventory_timeout_seconds: float = Field(2, gt=0)
    simulator_mode: Literal["NORMAL", "BUSY", "FLASH_SALE", "EXTREME"] = "NORMAL"
    simulator_events_per_second: int | None = Field(None, ge=1)
    simulator_product_count: int = Field(100, ge=1, le=1000)
    simulator_burst_duration_seconds: int = Field(0, ge=0)
    simulator_random_seed: int = 42
    simulator_event_mix: str = "STOCK_RESERVED:45,PURCHASE_COMPLETED:25,STOCK_RELEASED:20,INVENTORY_RESTOCKED:10"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
