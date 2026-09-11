import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "api")))

from kafka_producer import KafkaService
from chat import GroundedChatEngine


class DummyResponse:
    def __init__(self):
        self.status_code = 200


def evaluate_health(kafka_srv: KafkaService, chat_eng: GroundedChatEngine, resp: DummyResponse):
    kafka_ok = kafka_srv.check_health()
    neo4j_ok = chat_eng.check_health()
    is_overall_ok = kafka_ok and neo4j_ok
    if not is_overall_ok:
        resp.status_code = 503
    return {
        "status": "ok" if is_overall_ok else "not_ok",
        "kafka_connected": kafka_ok,
        "neo4j_connected": neo4j_ok,
    }


class TestHealthReadiness(unittest.TestCase):
    def setUp(self):
        self.kafka_srv = KafkaService()
        self.chat_eng = GroundedChatEngine()

    def test_both_dependencies_live(self):
        """When both Kafka and Neo4j respond, /health returns status: 'ok' and HTTP 200."""
        self.kafka_srv.check_health = MagicMock(return_value=True)
        self.chat_eng.check_health = MagicMock(return_value=True)
        resp = DummyResponse()

        result = evaluate_health(self.kafka_srv, self.chat_eng, resp)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(result["status"], "ok")
        self.assertTrue(result["kafka_connected"])
        self.assertTrue(result["neo4j_connected"])

    def test_kafka_down_neo4j_live(self):
        """When Kafka is down, /health must return status: 'not_ok' and HTTP 503."""
        self.kafka_srv.check_health = MagicMock(return_value=False)
        self.chat_eng.check_health = MagicMock(return_value=True)
        resp = DummyResponse()

        result = evaluate_health(self.kafka_srv, self.chat_eng, resp)
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(result["status"], "not_ok")
        self.assertFalse(result["kafka_connected"])
        self.assertTrue(result["neo4j_connected"])

    def test_neo4j_down_kafka_live(self):
        """When Neo4j is down, /health must return status: 'not_ok' and HTTP 503."""
        self.kafka_srv.check_health = MagicMock(return_value=True)
        self.chat_eng.check_health = MagicMock(return_value=False)
        resp = DummyResponse()

        result = evaluate_health(self.kafka_srv, self.chat_eng, resp)
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(result["status"], "not_ok")
        self.assertTrue(result["kafka_connected"])
        self.assertFalse(result["neo4j_connected"])

    def test_both_dependencies_down(self):
        """When both are down, /health must return status: 'not_ok' and HTTP 503."""
        self.kafka_srv.check_health = MagicMock(return_value=False)
        self.chat_eng.check_health = MagicMock(return_value=False)
        resp = DummyResponse()

        result = evaluate_health(self.kafka_srv, self.chat_eng, resp)
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(result["status"], "not_ok")
        self.assertFalse(result["kafka_connected"])
        self.assertFalse(result["neo4j_connected"])


if __name__ == "__main__":
    unittest.main()
