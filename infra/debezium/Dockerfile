# =============================================================================
# Debezium Kafka Connect image with:
#   - Debezium PostgreSQL source connector (reads WAL)
#   - ClickHouse JDBC sink connector (writes to ClickHouse)
#
# confluent-hub install handles downloading + installing connector JARs
# into /usr/share/confluent-hub-components inside the container.
# =============================================================================

FROM confluentinc/cp-kafka-connect:7.6.0

USER root

# Install ClickHouse JDBC driver dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

USER appuser

# Debezium PostgreSQL connector — the WAL reader
# Uses pgoutput (built-in to PG10+), no Postgres extension needed
RUN confluent-hub install --no-prompt \
    debezium/debezium-connector-postgresql:2.5.4

# ClickHouse Kafka Connect JDBC sink
# Allows Kafka → ClickHouse streaming via JDBC
RUN confluent-hub install --no-prompt \
    clickhouse/clickhouse-kafka-connect:v1.0.15

# Lenses.io HTTP Sink — alternative sink for webhook-style downstream services
RUN confluent-hub install --no-prompt \
    lensesio/kafka-connect-http-sink:1.3.2