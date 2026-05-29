from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_port: int = 8010
    log_level: str = "INFO"
    kafka_bootstrap_servers: str = "kafka-1:29092"
    kafka_topic: str = "warehouse-events"
    kafka_dlq_topic: str = "warehouse-events-dlq"
    kafka_group_id: str = "warehouse-state-consumer"
    cassandra_contact_points: str = "cassandra-1,cassandra-2,cassandra-3"
    cassandra_keyspace: str = "warehouse"
    cassandra_write_consistency: str = "QUORUM"
    cassandra_read_consistency: str = "ONE"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
