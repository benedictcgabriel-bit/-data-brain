import datetime
import json
import logging
import os
import sys
import time
from typing import Any, Dict
try:
    from kafka import KafkaConsumer
except ImportError:
    KafkaConsumer = None

try:
    from neo4j import GraphDatabase, Driver
except ImportError:
    GraphDatabase = None
    Driver = None

try:
    import requests
except ImportError:
    requests = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("loader.consumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "csv-rows")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "csv-loader-group")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "hackathon_secret_password_2026")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

INTERNAL_API_URL = os.getenv("INTERNAL_API_URL", "http://api:8000")


def wait_for_neo4j() -> Driver:
    """
    Retry loop handling Neo4j Bolt initialization and authentication readiness.
    """
    logger.info(f"Connecting to Neo4j at {NEO4J_URI}...")
    driver = None
    for attempt in range(1, 31):
        try:
            driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USER, NEO4J_PASSWORD),
                max_connection_lifetime=300,
            )
            with driver.session(database=NEO4J_DATABASE) as session:
                res = session.run("RETURN 1 AS ready").single()
                if res and res["ready"] == 1:
                    logger.info("Neo4j database connection established.")
                    # Initialize constraints
                    init_constraints(driver)
                    return driver
        except Exception as e:
            logger.info(f"Waiting for Neo4j readiness (attempt {attempt}/30): {e}")
            time.sleep(2)

    logger.error("Failed to connect to Neo4j within retry limit.")
    sys.exit(1)


def init_constraints(driver: Driver):
    """
    Creates uniqueness constraints for Dataset and Row identity.
    """
    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            session.run(
                "CREATE CONSTRAINT dataset_id_unique IF NOT EXISTS "
                "FOR (d:Dataset) REQUIRE d.id IS UNIQUE"
            )
            session.run(
                "CREATE CONSTRAINT row_identity_unique IF NOT EXISTS "
                "FOR (r:Row) REQUIRE (r.dataset_id, r.row_index) IS UNIQUE"
            )
        logger.info("Neo4j constraints verified/created.")
    except Exception as e:
        logger.warning(f"Could not verify constraints (may be non-enterprise or already existing): {e}")


def wait_for_kafka() -> KafkaConsumer:
    """
    Retry loop handling Kafka KRaft broker readiness and leader election.
    """
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} for topic '{KAFKA_TOPIC}'...")
    for attempt in range(1, 31):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            logger.info("Kafka consumer initialized successfully.")
            return consumer
        except Exception as e:
            logger.info(f"Waiting for Kafka leader election (attempt {attempt}/30): {e}")
            time.sleep(2)

    logger.error("Failed to connect to Kafka within retry limit.")
    sys.exit(1)


def report_progress(job_id: str, loaded_delta: int = 0, failed_delta: int = 0):
    try:
        url = f"{INTERNAL_API_URL}/internal/progress"
        payload = {
            "job_id": job_id,
            "loaded_delta": loaded_delta,
            "failed_delta": failed_delta,
            "mark_failed": False,
        }
        requests.post(url, json=payload, timeout=3)
    except Exception as e:
        logger.warning(f"Could not report progress for job {job_id}: {e}")


CYPHER_MERGE_ROW = """
MERGE (d:Dataset {id: $dataset_id})
ON CREATE SET
  d.filename = $filename,
  d.uploaded_at = $uploaded_at
ON MATCH SET
  d.last_accessed_at = $uploaded_at
MERGE (r:Row {
  dataset_id: $dataset_id,
  row_index: $row_index
})
SET r += $columns
MERGE (d)-[:HAS_ROW]->(r)
"""


def persist_row(driver: Driver, message: Dict[str, Any]) -> bool:
    """
    Executes idempotent Cypher MERGE query for a single row.
    """
    job_id = message["job_id"]
    dataset_id = message["dataset_id"]
    filename = message["filename"]
    row_index = message["row_index"]
    columns = message["columns"]

    now_iso = datetime.datetime.utcnow().isoformat() + "Z"

    with driver.session(database=NEO4J_DATABASE) as session:
        session.run(
            CYPHER_MERGE_ROW,
            dataset_id=dataset_id,
            filename=filename,
            uploaded_at=now_iso,
            row_index=row_index,
            columns=columns,
        )
    return True


def run():
    logger.info("Starting Kafka -> Neo4j Loader Worker...")
    driver = wait_for_neo4j()
    consumer = wait_for_kafka()

    logger.info("Loader worker running. Waiting for row messages on topic 'csv-rows'...")
    while True:
        try:
            # Poll messages
            msg_pack = consumer.poll(timeout_ms=1000)
            if not msg_pack:
                time.sleep(0.1)
                continue

            for tp, messages in msg_pack.items():
                for msg in messages:
                    data = msg.value
                    job_id = data.get("job_id")
                    try:
                        persist_row(driver, data)
                        if job_id:
                            report_progress(job_id, loaded_delta=1, failed_delta=0)
                    except Exception as err:
                        logger.error(f"Failed to persist row {data.get('row_index')}: {err}")
                        if job_id:
                            report_progress(job_id, loaded_delta=0, failed_delta=1)

        except Exception as e:
            logger.error(f"Consumer loop exception: {e}")
            time.sleep(2)


if __name__ == "__main__":
    run()
