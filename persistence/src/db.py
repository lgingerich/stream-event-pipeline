from datetime import datetime, timezone
import json
from loguru import logger
import psycopg2

class DB:
    def __init__(self, db_name, db_user, db_password, db_host, db_port):
        self.conn = None # Initialize conn to None
        try:
            logger.info(f"Attempting to connect to database {db_name} at {db_host}:{db_port}")
            self.conn = psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port
            )
            logger.info("Database connection successful")
            self.create_tables() # Also create tables on db initialization
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise e

    def create_tables(self):
        """
        Create events table

        Rather than having distinct fields for decoded events, pack all the decoded
        data into a JSONB field. Depending on use case, this is not the perfect
        solution, but it does generalize across all event types.
        """
        try:
            cur = self.conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    block_hash            VARCHAR(66),
                    block_number          INTEGER,
                    block_timestamp       INTEGER,
                    transaction_hash      VARCHAR(66),
                    transaction_index     INTEGER,
                    log_index             INTEGER,
                    contract_address      VARCHAR(42),
                    data                  VARCHAR,
                    topics                VARCHAR(66)[],
                    removed               BOOLEAN,
                    event                 VARCHAR,
                    contract              VARCHAR,
                    decoded_data          JSONB,
                    insert_timestamp      TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (transaction_hash, log_index)
                )
            """)
            # Create basic indexes that we may want. Would also be good to add one to 
            # decoded_data as querying on event name is likely common.
            cur.execute("CREATE INDEX IF NOT EXISTS idx_block_number ON events (block_number)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_contract_address ON events (contract_address)")
            self.conn.commit()
            logger.info("Tables created successfully")
        except Exception as e:
            self.conn.rollback()
            logger.error("Error creating tables: {}", e)
            raise e

    def insert_data(self, data):
        try:
            data_to_insert = data.copy()
            data_to_insert['insert_timestamp'] = datetime.now(timezone.utc)
            data_to_insert['decoded_data'] = json.dumps(data_to_insert['decoded_data'])

            self.conn.cursor().execute("""
                INSERT INTO events (
                    block_hash, block_number, block_timestamp, transaction_hash,
                    transaction_index, log_index, contract_address, data,
                    topics, removed, event, contract, decoded_data, insert_timestamp
                )
                VALUES (
                    %(block_hash)s, %(block_number)s, %(block_timestamp)s, %(transaction_hash)s,
                    %(transaction_index)s, %(log_index)s, %(contract_address)s, %(data)s,
                    %(topics)s, %(removed)s, %(event)s, %(contract)s, %(decoded_data)s,
                    %(insert_timestamp)s
                )
            """, data_to_insert)
            self.conn.commit()
            logger.info("Data inserted successfully for transaction: {}, log_index: {}", data['transaction_hash'], data['log_index'])
        except Exception as e:
            self.conn.rollback()
            logger.error("Error inserting data: {}", e)
            raise e

    def close(self):
        self.conn.close()