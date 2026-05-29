CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 3
}
AND durable_writes = true;

USE warehouse;

CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
  product_id text,
  zone_id text,
  available_quantity int,
  reserved_quantity int,
  last_event_ts timestamp,
  last_event_id uuid,
  supplier_id text,
  updated_at timestamp,
  PRIMARY KEY ((product_id), zone_id)
);

CREATE TABLE IF NOT EXISTS inventory_by_product (
  product_id text PRIMARY KEY,
  total_available int,
  total_reserved int,
  last_event_ts timestamp,
  last_event_id uuid,
  updated_at timestamp
);

CREATE TABLE IF NOT EXISTS inventory_by_zone (
  zone_id text,
  product_id text,
  available_quantity int,
  reserved_quantity int,
  last_event_ts timestamp,
  last_event_id uuid,
  supplier_id text,
  updated_at timestamp,
  PRIMARY KEY ((zone_id), product_id)
);

CREATE TABLE IF NOT EXISTS processed_events (
  event_id uuid PRIMARY KEY,
  event_type text,
  product_id text,
  processed_at timestamp,
  kafka_partition int,
  kafka_offset bigint
);

CREATE TABLE IF NOT EXISTS event_history_by_product (
  product_id text,
  event_ts timestamp,
  event_id uuid,
  event_type text,
  zone_id text,
  quantity int,
  payload text,
  PRIMARY KEY ((product_id), event_ts, event_id)
) WITH CLUSTERING ORDER BY (event_ts DESC, event_id ASC);

CREATE TABLE IF NOT EXISTS orders_by_id (
  order_id text PRIMARY KEY,
  status text,
  items text,
  created_at timestamp,
  updated_at timestamp,
  last_event_id uuid
);
