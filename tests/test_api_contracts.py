import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "api")))

from utils import parse_and_validate_csv, compute_dataset_id
from job_tracker import JobTracker


class TestApiContracts(unittest.TestCase):
    def setUp(self):
        self.tracker = JobTracker(persistence_file="/tmp/test_jobs.json")
        self.sample_csv = (
            b"Name,Department,Salary,City\n"
            b"Alice Johnson,Engineering,95000,New York\n"
            b"Bob Smith,HR,62000,Chicago\n"
            b"Charlie Brown,Marketing,71000,San Francisco\n"
        )

    def tearDown(self):
        if os.path.exists("/tmp/test_jobs.json"):
            os.remove("/tmp/test_jobs.json")

    def test_valid_csv_parsing(self):
        dataset_id, rows = parse_and_validate_csv(self.sample_csv, "test.csv")
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["Name"], "Alice Johnson")
        self.assertEqual(rows[0]["Salary"], 95000)
        self.assertEqual(rows[1]["Department"], "HR")
        self.assertTrue(len(dataset_id) == 64)

    def test_arbitrary_columns(self):
        custom_csv = (
            b"SensorId,Timestamp,Temperature_C,Pressure_hPa,BatteryStatus\n"
            b"SN-001,2026-09-11T10:00:00Z,23.5,1013.2,Good\n"
            b"SN-002,2026-09-11T10:01:00Z,24.1,1012.8,Normal\n"
        )
        dataset_id, rows = parse_and_validate_csv(custom_csv, "sensors.csv")
        self.assertEqual(len(rows), 2)
        self.assertIn("Temperature_C", rows[0])
        self.assertIn("BatteryStatus", rows[0])
        self.assertEqual(rows[0]["Temperature_C"], 23.5)

    def test_deterministic_dataset_id(self):
        id1 = compute_dataset_id(self.sample_csv)
        # Add CRLF instead of LF
        crlf_version = self.sample_csv.replace(b"\n", b"\r\n")
        id2 = compute_dataset_id(crlf_version)
        self.assertEqual(id1, id2, "Dataset ID must be identical regardless of line endings")

    def test_job_tracker_lifecycle(self):
        job = self.tracker.create_job("job-123", "dataset-abc", "data.csv", rows_total=10)
        self.assertEqual(job["status"], "queued")
        self.assertEqual(job["rows_total"], 10)
        self.assertEqual(job["rows_loaded"], 0)

        # Progress update 1: loading
        updated = self.tracker.update_progress("job-123", loaded_delta=4, failed_delta=0)
        self.assertEqual(updated["status"], "loading")
        self.assertEqual(updated["rows_loaded"], 4)

        # Progress update 2: completion
        updated = self.tracker.update_progress("job-123", loaded_delta=6, failed_delta=0)
        self.assertEqual(updated["status"], "complete")
        self.assertEqual(updated["rows_loaded"], 10)
        self.assertEqual(updated["rows_loaded"] + updated["rows_failed"], 10)

    def test_header_only_csv(self):
        header_only = b"Col1,Col2,Col3\n"
        dataset_id, rows = parse_and_validate_csv(header_only, "header_only.csv")
        self.assertEqual(len(rows), 0, "Header-only CSV must return 0 data rows")


if __name__ == "__main__":
    unittest.main()
