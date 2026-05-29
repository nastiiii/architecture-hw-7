# Smart Warehouse

Event-driven система склада: WMS producer -> Kafka `warehouse-events` -> stateful consumer -> Cassandra 3-node cluster. Схемы событий регистрируются в Schema Registry, плохие события уходят в `warehouse-events-dlq`.

## Что реализовано

- Kafka consumer group: `warehouse-state-consumer`.
- At-least-once: offset commit после обработки и записи в Cassandra.
- Cassandra tables: `inventory_by_product_zone`, `inventory_by_product`, `inventory_by_zone`, `processed_events`, `event_history_by_product`, `orders_by_id`.
- Идемпотентность по `event_id`.
- Logged batch для согласованного обновления денормализованных таблиц.
- Защита от out-of-order событий по `timestamp`.
- DLQ topic: `warehouse-events-dlq`.
- Cassandra 3-node cluster, RF=3, writes `QUORUM`, reads `ONE`.
- Schema Registry: Avro V1 и V2, backward compatibility, поле `supplier_id` в V2.

## Запуск

```bash
docker-compose down -v --remove-orphans
docker-compose up -d --build
docker-compose ps
```

Первый запуск Cassandra может занять несколько минут. Продолжать проверку нужно, когда `producer`, `consumer`, `kafka-1`, `schema-registry`, `cassandra-1`, `cassandra-2`, `cassandra-3` в статусе `healthy`.

## Проверка готовности

```bash
curl http://localhost:8000/health
curl http://localhost:8010/health
curl http://localhost:8081/subjects/warehouse-events-value/versions
docker exec cassandra-1 nodetool status
docker exec cassandra-1 cqlsh -e "DESCRIBE KEYSPACE warehouse;"
```

Ожидаемо:

- health endpoints возвращают `{"status":"ok"}`;
- Schema Registry возвращает `[1,2]`;
- `nodetool status` показывает 3 ноды `UN`;
- keyspace `warehouse` создан с `NetworkTopologyStrategy` и `datacenter1: 3`.

## Базовое событие

```bash
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' -d '{"event_id":"00000000-0000-0000-0000-000000001001","event_type":"PRODUCT_RECEIVED","product_id":"SKU-DEMO","zone_id":"ZONE-A","quantity":100,"timestamp":"2026-05-10T12:00:00Z"}'
```

```bash
docker exec cassandra-1 cqlsh -e "USE warehouse; SELECT * FROM inventory_by_product_zone WHERE product_id='SKU-DEMO' AND zone_id='ZONE-A';"
docker exec cassandra-1 cqlsh -e "USE warehouse; SELECT * FROM inventory_by_product WHERE product_id='SKU-DEMO';"
docker exec cassandra-1 cqlsh -e "USE warehouse; SELECT * FROM inventory_by_zone WHERE zone_id='ZONE-A';"
```

Ожидаемо: во всех трёх таблицах `SKU-DEMO`, `available = 100`, `reserved = 0`.

## Резервирование

```bash
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' -d '{"event_id":"00000000-0000-0000-0000-000000001002","event_type":"PRODUCT_RESERVED","product_id":"SKU-DEMO","zone_id":"ZONE-A","quantity":30,"timestamp":"2026-05-10T12:01:00Z"}'
docker exec cassandra-1 cqlsh -e "USE warehouse; SELECT * FROM inventory_by_product_zone WHERE product_id='SKU-DEMO' AND zone_id='ZONE-A';"
```

Ожидаемо: `available_quantity = 70`, `reserved_quantity = 30`.

## Идемпотентность

```bash
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' -d '{"event_id":"00000000-0000-0000-0000-000000001002","event_type":"PRODUCT_RESERVED","product_id":"SKU-DEMO","zone_id":"ZONE-A","quantity":30,"timestamp":"2026-05-10T12:01:00Z"}'
docker exec cassandra-1 cqlsh -e "USE warehouse; SELECT * FROM inventory_by_product_zone WHERE product_id='SKU-DEMO' AND zone_id='ZONE-A';"
```

Ожидаемо: значения не изменились, всё ещё `available_quantity = 70`, `reserved_quantity = 30`.

## Out-of-order

```bash
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' -d '{"event_id":"00000000-0000-0000-0000-000000001003","event_type":"PRODUCT_SHIPPED","product_id":"SKU-DEMO","zone_id":"ZONE-A","quantity":20,"timestamp":"2026-05-10T12:05:00Z"}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' -d '{"event_id":"00000000-0000-0000-0000-000000001004","event_type":"PRODUCT_RECEIVED","product_id":"SKU-DEMO","zone_id":"ZONE-A","quantity":50,"timestamp":"2026-05-10T12:02:00Z"}'
docker exec cassandra-1 cqlsh -e "USE warehouse; SELECT * FROM inventory_by_product_zone WHERE product_id='SKU-DEMO' AND zone_id='ZONE-A';"
```

Ожидаемо: старое событие `12:02` проигнорировано, состояние после shipment: `available_quantity = 50`, `reserved_quantity = 30`.

## DLQ

```bash
curl -X POST http://localhost:8000/events/raw -H 'Content-Type: application/json' -d '{"event_id":"00000000-0000-0000-0000-000000001005","event_type":"PRODUCT_SHIPPED","product_id":"SKU-DEMO","zone_id":"ZONE-A","quantity":-5,"timestamp":"2026-05-10T12:10:00Z","items":[],"schema_version":2,"supplier_id":null}'
docker exec -it hw6-servises-kafka-1-1 kafka-console-consumer --bootstrap-server kafka-1:29092 --topic warehouse-events-dlq --from-beginning --max-messages 1
```

Ожидаемо: сообщение в DLQ содержит `original_event`, `error_reason`, `error_code`, `failed_at`, `kafka_metadata`.

## Метрики

```bash
curl http://localhost:8010/metrics | grep -E "consumer_lag|events_processed_total|event_processing_duration_seconds|cassandra_write_errors_total"
```

Ожидаемо: есть `consumer_lag`, `events_processed_total`, `event_processing_duration_seconds`, `cassandra_write_errors_total`.

## Отказоустойчивость Cassandra

```bash
docker stop cassandra-2
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' -d '{"event_id":"00000000-0000-0000-0000-000000001006","event_type":"PRODUCT_RECEIVED","product_id":"SKU-FAILOVER","zone_id":"ZONE-A","quantity":200,"timestamp":"2026-05-10T12:20:00Z"}'
docker exec cassandra-1 cqlsh -e "USE warehouse; SELECT * FROM inventory_by_product_zone WHERE product_id='SKU-FAILOVER' AND zone_id='ZONE-A';"
docker start cassandra-2
docker exec cassandra-1 nodetool status
```

Ожидаемо: запись проходит при одной остановленной ноде, потому что writes используют `QUORUM`. После `docker start cassandra-2` подождать 30-90 секунд и повторить `nodetool status`, чтобы снова увидеть 3 `UN`.

## ДЗ7: CI/CD, Testing & Observability

Система расширена для ДЗ7 без замены предметной логики ДЗ6: остались два сервиса (`producer`, `consumer`), Kafka, Schema Registry и Cassandra, сверху добавлены CI, тесты, Prometheus, Grafana, Alertmanager, k6 и проверка SLI.

### Запуск всей системы

```bash
docker compose down -v --remove-orphans
docker compose up -d --build
python3 scripts/wait_for_stack.py
```

Адреса для демонстрации:

- Producer API: http://localhost:8000
- Consumer API: http://localhost:8010
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (`admin` / `admin`)
- Alertmanager: http://localhost:9093

### Тесты

Unit-тесты сервисов:

```bash
cd producer && pytest -q
cd ../aggregation_service && pytest -q
```

Integration и E2E тесты запускаются после `docker compose up`:

```bash
pip install -r tests/requirements.txt
pytest -q tests
```

Что проверяется:

- integration: `producer -> Kafka -> consumer -> Cassandra`, сценарий `PRODUCT_RECEIVED -> PRODUCT_RESERVED`;
- E2E: полный пользовательский сценарий `PRODUCT_RECEIVED -> ORDER_CREATED -> ORDER_COMPLETED` с проверкой HTTP-ответов и состояния Cassandra.

### CI pipeline

Конфигурация находится в `.github/workflows/ci.yml`. Pipeline запускается на `push` и `pull_request`.

Шаги:

1. build: `docker compose build`;
2. unit tests: отдельные тесты `producer` и `aggregation_service`;
3. integration tests: тесты взаимодействия сервисов через Kafka и Cassandra;
4. E2E tests: полный сценарий заказа;
5. load tests: k6, 10 VU, 30 секунд, thresholds `http_req_failed < 1%`, `p95 < 500ms`;
6. metrics validation: Prometheus API проверяет SLI-пороги;
7. artifacts: сохраняются `docker-compose.log`, `docker-compose-ps.txt`, `k6-summary.json`, `prometheus-sli-results.json`.

Pipeline падает при ошибках build, тестов, k6 thresholds или SLI-проверок.

### Prometheus метрики

Оба сервиса экспортируют `/metrics` в Prometheus-формате.

Базовые HTTP-метрики:

- `http_requests_total{method,endpoint,status}` — количество запросов;
- `http_request_errors_total{method,endpoint,error_type}` — ошибки;
- `http_request_duration_seconds{method,endpoint}` — histogram latency.

Consumer дополнительно экспортирует предметные метрики:

- `events_processed_total{event_type}`;
- `event_processing_duration_seconds`;
- `cassandra_write_errors_total`;
- `consumer_lag{partition}`.

Prometheus scrape config: `infra/prometheus/prometheus.yml`.

### Grafana dashboards

Grafana автоматически поднимает datasource и dashboards через provisioning:

- `infra/grafana/dashboards/producer-dashboard.json`;
- `infra/grafana/dashboards/consumer-dashboard.json`;
- `infra/grafana/dashboards/infrastructure-dashboard.json`.

На сервисных дашбордах есть throughput, error rate, p50/p95/p99 latency и availability/event processing latency. На инфраструктурном дашборде есть Kafka consumer lag, Kafka partitions, Cassandra container CPU и Cassandra container memory.

### Load tests

k6 сценарий находится в `load-tests/warehouse_load.js`.

Локальный запуск:

```bash
docker run --rm --network host \
  -e PRODUCER_URL=http://127.0.0.1:8000 \
  -v "$PWD/load-tests:/scripts:ro" \
  -v "$PWD/artifacts:/artifacts" \
  grafana/k6:0.54.0 run --summary-export /artifacts/k6-summary.json /scripts/warehouse_load.js
```

### Alerts

Alert rules лежат в `infra/prometheus/alerts.yml`, Alertmanager config — `infra/alertmanager/alertmanager.yml`.

Реализованные алерты:

- `ServiceTargetDown`: `up{job=~"producer|consumer"} == 0`;
- `HighHttpErrorRate`: error rate сервиса выше 5%;
- `HighP95Latency`: p95 latency выше 1 секунды;
- `SystemAvailabilityFailureThreshold`: availability producer API ниже 95%.

Для демонстрации firing-состояния можно временно остановить сервис:

```bash
docker compose stop producer
```

Через 30 секунд алерт `ServiceTargetDown` появится в Prometheus Alerts и Alertmanager. После демонстрации:

```bash
docker compose start producer
```

### System-level SLI/SLO

SLI считаются из реальных Prometheus метрик и используются в `scripts/check_prometheus_sli.py`. В CI скрипт падает с exit code 1, если порог нарушен.

| SLI | PromQL | SLO | Порог отказа | Обоснование |
| --- | --- | --- | --- | --- |
| API availability | `sum(rate(http_requests_total{job="producer",status!~"5.."}[5m])) / sum(rate(http_requests_total{job="producer"}[5m]))` | `> 99.5%` | `< 95%` | Producer — входная точка всей системы; 5xx означает, что события не попадают в Kafka. |
| API latency p95 | `histogram_quantile(0.95, sum by (le) (http_request_duration_seconds_bucket{job="producer",endpoint="/events"}))` | `< 500ms` | `> 1000ms` | Публикация события должна быть быстрой; задержка выше 1 секунды уже заметна клиенту и указывает на проблемы Kafka/producer. |
| Event processing delay | `sum(consumer_lag)` | `< 100 messages` в спокойном режиме | `> 2000 messages` под CI-load | Lag показывает, успевает ли consumer обрабатывать поток событий и обновлять Cassandra; порог отказа выше SLO, потому что k6 создает короткий всплеск нагрузки. |

Проверка SLI:

```bash
python3 scripts/check_prometheus_sli.py
cat artifacts/prometheus-sli-results.json
```
