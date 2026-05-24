import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import sys
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connection parameters. When running inside docker network, host is 'postgres'.
# When running on host machine, use localhost and port 5433 (as per docker-compose).
DB_HOST = "localhost" # Default to localhost, can be overridden via env
DB_PORT = "5433"      # Default external port
DB_NAME = "ordersdb"
DB_USER = "orderuser"
DB_PASS = "orderpass"

# Use the 'test_decoding' plugin to receive human-readable text representations of WAL changes
PLUGIN = "test_decoding"
SLOT_NAME = "manual_python_slot"

def create_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connection_factory=LogicalReplicationConnection
    )

def consume_wal():
    logger.info("Attempting to connect to PostgreSQL logical replication...")
    try:
        conn = create_connection()
        cur = conn.cursor()
        logger.info(f"Connected successfully to {DB_NAME} at {DB_HOST}:{DB_PORT}")

        # Try to create the logical replication slot.
        # It may already exist if this script was run before.
        try:
            cur.create_replication_slot(SLOT_NAME, output_plugin=PLUGIN)
            logger.info(f"Created replication slot '{SLOT_NAME}' using '{PLUGIN}' plugin.")
        except psycopg2.errors.DuplicateObject:
            logger.info(f"Replication slot '{SLOT_NAME}' already exists, reusing it.")
            conn.rollback() # Rollback the failed create transaction to continue

        logger.info("Starting to consume WAL stream. Waiting for changes...")
        
        # Start replication, asking only for changes from the 'outbox_events' table 
        # Note: test_decoding does not filter by PUBLICATION like pgoutput does natively,
        # but it will stream all changes. We will filter in Python.
        cur.start_replication(slot_name=SLOT_NAME, decode=True)

        def process_message(msg):
            # The payload from test_decoding is a string like:
            # table public.outbox_events: INSERT: id[uuid]:... payload[jsonb]:...
            payload = msg.payload
            
            # Filter specifically for the outbox_events table
            if "public.outbox_events" in payload:
                logger.info(f"OUTBOX EVENT DETECTED:\n{payload}")
            else:
                logger.debug(f"Ignored WAL event for other table: {payload}")

            # Send feedback to Postgres to advance the replication slot.
            # This tells Postgres we have successfully processed up to this point,
            # allowing it to delete old WAL segments.
            msg.cursor.send_feedback(flush_lsn=msg.data_start)

        # Start the infinite consumption loop
        cur.consume_stream(process_message)

    except KeyboardInterrupt:
        logger.info("Manual interruption received. Stopping consumer...")
    except Exception as e:
        logger.error(f"Error consuming stream: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == '__main__':
    import os
    # Allow overriding connection details via environment variables (e.g. for Docker)
    DB_HOST = os.getenv("DB_HOST", DB_HOST)
    DB_PORT = os.getenv("DB_PORT", DB_PORT)
    consume_wal()
