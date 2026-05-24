import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import struct
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

# Use the 'pgoutput' plugin (PostgreSQL's native logical replication plugin)
PLUGIN = "pgoutput"
SLOT_NAME = "manual_python_slot_pgoutput"

def create_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connection_factory=LogicalReplicationConnection
    )

def parse_insert_message(payload: bytes):
    """
    Parses a pgoutput Insert ('I') message.
    Format:
    Byte1('I')
    Int32(Relation ID)
    Byte1('N') - new tuple
    Int16(Number of columns)
    [For each column]:
        Byte1('t' / 'n' / 'u')
        If 't', Int32(Length) + String(Data)
    """
    offset = 1 # Skip 'I'
    
    # Read Relation ID (4 bytes, Big-Endian)
    rel_id = struct.unpack_from('>I', payload, offset)[0]
    offset += 4
    
    # Read tuple type (1 byte, usually 'N')
    tuple_type = payload[offset:offset+1]
    offset += 1
    
    # Read number of columns (2 bytes, Big-Endian)
    num_cols = struct.unpack_from('>H', payload, offset)[0]
    offset += 2
    
    columns = []
    for _ in range(num_cols):
        col_type = payload[offset:offset+1]
        offset += 1
        
        if col_type == b't':
            # Text formatted value
            col_len = struct.unpack_from('>I', payload, offset)[0]
            offset += 4
            col_data = payload[offset:offset+col_len]
            offset += col_len
            columns.append(col_data.decode('utf-8'))
        elif col_type == b'n':
            columns.append(None)
        elif col_type == b'u':
            columns.append("UNCHANGED_TOAST")
            
    return rel_id, columns


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
        
        # Start replication, asking only for changes from the 'outbox_pub' publication.
        # pgoutput filters events at the source!
        options = {
            'proto_version': '1',
            'publication_names': 'outbox_publication'
        }
        cur.start_replication(slot_name=SLOT_NAME, options=options, decode=False)

        def process_message(msg):
            # The payload from pgoutput is a raw binary buffer (bytes).
            # The first byte indicates the message type.
            payload = msg.payload
            msg_type = payload[0:1]
            
            if msg_type == b'R':
                logger.debug("Relation message received (schema definition).")
            elif msg_type == b'B':
                logger.debug("Begin transaction.")
            elif msg_type == b'C':
                logger.debug("Commit transaction.")
            elif msg_type == b'I':
                try:
                    rel_id, cols = parse_insert_message(payload)
                    logger.info(f"OUTBOX EVENT DETECTED (INSERT)! RelID: {rel_id}, Columns: {cols}")
                except Exception as e:
                    logger.error(f"Failed to parse INSERT message: {e}. Raw: {payload[:50]}")
            elif msg_type == b'U':
                logger.info(f"OUTBOX EVENT DETECTED (UPDATE)! Raw binary: {payload[:100]}...")
            elif msg_type == b'D':
                logger.info(f"OUTBOX EVENT DETECTED (DELETE)! Raw binary: {payload[:100]}...")
            else:
                logger.debug(f"Other message type: {msg_type}")

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
