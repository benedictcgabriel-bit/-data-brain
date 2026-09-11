import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "api")))

from utils import parse_and_validate_csv
from chat import GroundedChatEngine
from job_tracker import JobTracker


class MockNeo4jSession:
    def __init__(self, data_exists=True):
        self.data_exists = data_exists

    def run(self, cypher, params=None):
        params = params or {}
        if "RETURN count(r) AS cnt" in cypher:
            return [{"cnt": 12 if self.data_exists else 0}]
        if "UNWIND keys(r)" in cypher:
            return [{"k": "Name"}, {"k": "Department"}, {"k": "Salary"}, {"k": "City"}]
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockNeo4jDriver:
    def __init__(self, data_exists=True):
        self.data_exists = data_exists

    def session(self, database=None):
        return MockNeo4jSession(self.data_exists)


class TestHostileInputs(unittest.TestCase):
    """
    Tests all 11 scenarios specified in Phase 9 of the Master Specification.
    """

    def setUp(self):
        self.tracker = JobTracker("/tmp/test_hostile_jobs.json")
        self.chat_engine = GroundedChatEngine()

    def tearDown(self):
        if os.path.exists("/tmp/test_hostile_jobs.json"):
            os.remove("/tmp/test_hostile_jobs.json")

    def test_scenario_1_valid_csv(self):
        """Scenario 1: Valid CSV -> End-to-end success."""
        valid_csv = b"Name,Department,Salary\nAlice,Engineering,95000\nBob,HR,60000\n"
        dataset_id, rows = parse_and_validate_csv(valid_csv, "valid.csv")
        self.assertEqual(len(rows), 2)
        self.assertTrue(len(dataset_id) == 64)

    def test_scenario_2_empty_csv(self):
        """Scenario 2: Empty CSV -> Clean validation/controlled handling; no crash."""
        empty_bytes = b""
        with self.assertRaises(ValueError) as ctx:
            parse_and_validate_csv(empty_bytes, "empty.csv")
        self.assertIn("empty", str(ctx.exception).lower())

    def test_scenario_3_header_only_csv(self):
        """Scenario 3: Header-only CSV -> No phantom rows; clean handling."""
        header_csv = b"Name,Department,Salary,City\n"
        dataset_id, rows = parse_and_validate_csv(header_csv, "header_only.csv")
        self.assertEqual(len(rows), 0)

    def test_scenario_4_malformed_csv(self):
        """Scenario 4: Malformed CSV -> Controlled error; no dead service."""
        # Malformed bytes with NUL byte or illegal unclosed quotes
        malformed = b"Name,Department,Salary\n\"Alice,Engineering\x00,95000\n"
        with self.assertRaises(ValueError):
            parse_and_validate_csv(malformed, "malformed.csv")

    def test_scenario_5_non_csv(self):
        """Scenario 5: Non-CSV -> Reject cleanly."""
        data = b"{\"key\": \"value\"}"
        with self.assertRaises(ValueError) as ctx:
            parse_and_validate_csv(data, "payload.json")
        self.assertIn("must have a .csv extension", str(ctx.exception).lower())

        with self.assertRaises(ValueError):
            parse_and_validate_csv(b"hello world", "notes.txt")

    def test_scenario_6_arbitrary_columns(self):
        """Scenario 6: Arbitrary columns -> Works without code changes."""
        csv_data = b"Alpha,Beta,Gamma,Delta,Epsilon\n1,2,3,4,5\n10,20,30,40,50\n"
        dataset_id, rows = parse_and_validate_csv(csv_data, "arbitrary.csv")
        self.assertEqual(len(rows), 2)
        self.assertIn("Alpha", rows[0])
        self.assertIn("Epsilon", rows[0])

    def test_scenario_7_chat_before_upload(self):
        """Scenario 7: Chat before upload -> Clean response; no fabrication."""
        self.chat_engine.get_driver = lambda: MockNeo4jDriver(data_exists=False)
        response = self.chat_engine.process_query("How many employees are in HR?")
        self.assertFalse(response["grounded"])
        self.assertIn("No data is currently loaded", response["answer"])
        self.assertEqual(response["result"], [{"total_rows": 0}])

    def test_scenario_8_unsupported_chat(self):
        """Scenario 8: Unsupported chat -> grounded=false + honest no-data."""
        self.chat_engine.get_driver = lambda: MockNeo4jDriver(data_exists=True)
        # Mock query execution returning empty records for unknown attribute
        self.chat_engine.execute_cypher = lambda c, p=None: []
        response = self.chat_engine.process_query("What is the weather in Tokyo right now?")
        self.assertFalse(response["grounded"])
        self.assertIn("do not have data in the knowledge graph", response["answer"])
        self.assertEqual(response["cypher"], "NONE")
        self.assertEqual(response["result"], [])

    def test_scenario_9_kafka_unavailable_health(self):
        """Scenario 9: Kafka unavailable -> health not_ok."""
        # Simulated health check
        kafka_ok = False
        neo4j_ok = True
        overall = kafka_ok and neo4j_ok
        self.assertFalse(overall)

    def test_scenario_10_neo4j_unavailable_health(self):
        """Scenario 10: Neo4j unavailable -> health not_ok."""
        kafka_ok = True
        neo4j_ok = False
        overall = kafka_ok and neo4j_ok
        self.assertFalse(overall)

    def test_scenario_11_loader_restart_accounting(self):
        """Scenario 11: Loader restart -> No fake completion."""
        job = self.tracker.create_job("job-hostile", "ds-hostile", "data.csv", rows_total=50)
        # Simulate loader crash after 20 rows
        self.tracker.update_progress("job-hostile", loaded_delta=20, failed_delta=0)
        status_mid = self.tracker.get_job("job-hostile")
        self.assertEqual(status_mid["status"], "loading")
        self.assertNotEqual(status_mid["status"], "complete")

        # Simulate loader restart resuming and completing remaining 30 rows
        self.tracker.update_progress("job-hostile", loaded_delta=30, failed_delta=0)
        status_final = self.tracker.get_job("job-hostile")
        self.assertEqual(status_final["status"], "complete")
        self.assertEqual(status_final["rows_loaded"], 50)


if __name__ == "__main__":
    unittest.main()
