from functools import lru_cache
from decimal import Decimal
from typing import Literal

from pydantic import Field, model_validator
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
    kafka_pricing_topic: str = "pricing-events"
    pricing_enabled: bool = True
    pricing_window_seconds: int = Field(30, ge=10, le=300)
    pricing_cooldown_seconds: int = Field(30, ge=1)
    pricing_reversal_seconds: int = Field(120, ge=1)
    pricing_min_price: Decimal = Field(Decimal("0.01"), gt=0)
    pricing_max_price: Decimal = Field(Decimal("99999999.99"), gt=0, le=Decimal("99999999.99"))
    pricing_max_increase: Decimal = Field(Decimal("0.20"), ge=0, le=1)
    pricing_max_discount: Decimal = Field(Decimal("0.20"), ge=0, lt=1)
    pricing_max_step: Decimal = Field(Decimal("0.05"), gt=0, le=Decimal("0.20"))
    consumer_max_attempts: int = Field(3, ge=1)
    consumer_retry_seconds: float = Field(0.5, ge=0)
    consumer_instances: int = Field(1, ge=1, le=6)
    consumer_batch_size: int = Field(50, ge=1, le=500)
    consumer_batch_enabled: bool = True
    consumer_poll_ms: int = Field(50, ge=1, le=1000)
    shutdown_timeout_seconds: float = Field(30, gt=0)
    database_pool_size: int = Field(10, ge=1, le=100)
    database_max_overflow: int = Field(5, ge=0, le=100)
    websocket_queue_size: int = Field(100, ge=1, le=10000)
    websocket_send_timeout_seconds: float = Field(5, gt=0)
    logging_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR'] = 'INFO'
    websocket_heartbeat_seconds: float = Field(15, gt=0)
    app_env: Literal["development", "test", "production"] = "production"
    enable_chaos: bool = False
    chaos_token: str = ""
    enable_retail_controls: bool = False
    forecast_bucket_seconds: int = Field(5, ge=5, le=5)
    forecast_interval_seconds: int = Field(5, ge=5, le=300)
    forecast_stale_seconds: int = Field(30, ge=10)
    forecast_safety_stock: int = Field(10, ge=0, le=500)
    forecast_max_restock: int = Field(500, ge=1, le=10000)
    forecast_model_path: str = "/app/artifacts/demand-model.json"
    analyst_base_url: str = "https://api.openai.com/v1"
    analyst_model: str = ""
    analyst_api_key: str = ""
    analyst_enabled: bool = False
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

    @model_validator(mode="after")
    def pricing_limits(self):
        if self.analyst_base_url != "https://api.openai.com/v1" and self.analyst_api_key and not self.analyst_base_url.startswith("https://"):
            raise ValueError("Remote analyst endpoint must use HTTPS")
        if self.consumer_instances > self.kafka_inventory_partitions:
            raise ValueError("consumer instances must not exceed inventory partition count")
        if self.pricing_min_price > self.pricing_max_price:
            raise ValueError("pricing minimum must not exceed maximum")
        if self.pricing_reversal_seconds < self.pricing_cooldown_seconds:
            raise ValueError("pricing reversal interval must cover the cooldown")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
