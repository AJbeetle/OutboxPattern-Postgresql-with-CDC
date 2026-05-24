# =============================================================================
# infra/debezium/Dockerfile
#
# WHY we extend cp-kafka-connect instead of using debezium/connect:
#   The Confluent cp-kafka-connect base image is more stable for mixing
#   Debezium (source) and Confluent (sink) connectors in one worker.
#   debezium/connect only ships source connectors.
#
# HOW confluent-hub install works:
#   Downloads the connector ZIP from Confluent Hub, extracts the JAR files
#   into /usr/share/confluent-hub-components/{connector-name}/, and adds
#   them to the plugin path that Kafka Connect scans at startup.
#   After this, you register connector instances via REST API — the JARs
#   are just the available plugin classes.
#
# Connectors installed:
#   1. debezium-connector-postgresql
#      SOURCE: reads PostgreSQL WAL via pgoutput logical replication protocol.
#      Includes the Outbox Event Router SMT (Single Message Transform).
#
#   2. kafka-connect-jdbc
#      SINK: writes Kafka messages to any JDBC database (Postgres, MySQL, etc.)
#      Ships with the PostgreSQL JDBC driver (org.postgresql.Driver).
#      Does INSERT ... ON CONFLICT upserts when configured with pk.mode=record_value.
# =============================================================================

FROM confluentinc/cp-kafka-connect:7.6.0

USER root
RUN microdnf update && microdnf install -y curl wget \
    && microdnf clean all
USER appuser

RUN confluent-hub install --no-prompt \
    debezium/debezium-connector-postgresql:2.5.4

RUN confluent-hub install --no-prompt \
    confluentinc/kafka-connect-jdbc:10.7.6