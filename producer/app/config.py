from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_port: int = 8000
    log_level: str = "INFO"
    kafka_bootstrap_servers: str = "kafka-1:29092,kafka-2:29092"
    kafka_topic: str = "warehouse-events"
    schema_registry_url: str = "http://schema-registry:8081"
    partition_key_field: str = "product_id"
    producer_acks: str = "all"
    producer_max_retries: int = 5
    producer_backoff_seconds: float = 0.5
    generator_enabled: bool = True
    generator_interval_seconds: float = 1.0

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
