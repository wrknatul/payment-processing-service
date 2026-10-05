from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    api_key: str = "dev-api-key"

    webhook_timeout_seconds: float = Field(default=5.0, gt=0)
    webhook_signing_secret: str = "dev-webhook-secret"

    # Total number of processing attempts per message, the first delivery included.
    max_attempts: int = Field(default=3, ge=1)
    retry_base_delay_seconds: float = Field(default=5.0, gt=0)
    retry_backoff_multiplier: float = Field(default=2.0, ge=1)
    consumer_prefetch: int = Field(default=10, ge=1)

    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1)

    gateway_min_delay_seconds: float = Field(default=2.0, ge=0)
    gateway_max_delay_seconds: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
