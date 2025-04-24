from datetime import datetime, timezone
from loguru import logger
import json


class DB:
    def __init__(self, conn):
        self.conn = conn
        self.create_tables()

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