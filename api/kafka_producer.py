import json
import logging
import os
import time
from typing import Any, Dict, Optional
try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient
except ImportError:
    KafkaProducer = None
    KafkaAdminClient = None

logger = logging.getLogger("api.kafka_producer")


class KafkaService:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "csv-rows")
        self._producer: Optional[KafkaProducer] = None

    def get_producer(self) -> Optional[KafkaProducer]:
        if self._producer is not None:
            return self._producer
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=5,
                request_timeout_ms=10000,
            )
            logger.info("Connected to Kafka Producer successfully.")
            return self._producer
        except Exception as e:
            logger.warning(f"Kafka producer connection error: {e}")
            return None

    def publish_row(self, message: Dict[str, Any]) -> bool:
        producer = self.get_producer()
        if not producer:
            raise ConnectionError(f"Cannot publish message: Kafka broker ({self.bootstrap_servers}) unreachable.")
        key = message.get("dataset_id")
        future = producer.send(self.topic, key=key, value=message)
        # Flush or wait for delivery
        future.get(timeout=10)
        return True

    def check_health(self) -> bool:
        """
        Actively checks if Kafka broker is genuinely reachable and responsive.
        """
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                request_timeout_ms=3000,
            )
            topics = admin.list_topics()
            admin.close()
            return True
        except Exception as e:
            logger.debug(f"Kafka health check failed: {e}")
            return False


kafka_service = KafkaService()
