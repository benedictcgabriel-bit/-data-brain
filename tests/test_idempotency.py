import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "api")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "loader")))

from utils import parse_and_validate_csv, compute_dataset_id


class InMemGraphDB:
    """
    In-memory simulation of Neo4j MERGE semantics for Dataset and Row entities.
    Enforces uniqueness constraints and exact MERGE behavior matching Cypher queries.
    """

    def __init__(self):
        # Keyed by dataset_id -> Dataset properties
        self.datasets = {}
        # Keyed by (dataset_id, row_index) -> Row properties
        self.rows = {}
        # Set of (dataset_id, (dataset_id, row_index)) edges
        self.has_row_edges = set()

    def merge_row(self, dataset_id: str, filename: str, row_index: int, columns: dict, uploaded_at: str):
        # Step 1: MERGE (d:Dataset {id: $dataset_id})
        if dataset_id not in self.datasets:
            self.datasets[dataset_id] = {
                "id": dataset_id,
                "filename": filename,
                "uploaded_at": uploaded_at,
            }
        else:
            self.datasets[dataset_id]["last_accessed_at"] = uploaded_at

        # Step 2: MERGE (r:Row {dataset_id: $dataset_id, row_index: $row_index}) SET r += $columns
        row_key = (dataset_id, row_index)
        if row_key not in self.rows:
            self.rows[row_key] = {"dataset_id": dataset_id, "row_index": row_index}
        self.rows[row_key].update(columns)

        # Step 3: MERGE (d)-[:HAS_ROW]->(r)
        edge = (dataset_id, row_key)
        self.has_row_edges.add(edge)

    def get_counts(self):
        return {
            "dataset_count": len(self.datasets),
            "row_count": len(self.rows),
            "has_row_count": len(self.has_row_edges),
        }


class TestIdempotency(unittest.TestCase):
    def setUp(self):
        self.db = InMemGraphDB()
        self.employees_csv = (
            b"Name,Department,Salary,City\n"
            b"Alice Johnson,Engineering,95000,New York\n"
            b"Bob Smith,HR,62000,Chicago\n"
            b"Charlie Brown,Marketing,71000,San Francisco\n"
            b"Diana Prince,Engineering,105000,Seattle\n"
        )

    def test_exact_same_csv_twice_idempotency(self):
        """
        Tests that uploading the exact same CSV twice leaves graph counts strictly identical.
        """
        # Run 1
        ds_id_1, rows_1 = parse_and_validate_csv(self.employees_csv, "employees.csv")
        for idx, r in enumerate(rows_1):
            self.db.merge_row(ds_id_1, "employees.csv", idx, r, "2026-09-11T11:00:00Z")

        counts_run_1 = self.db.get_counts()
        self.assertEqual(counts_run_1["dataset_count"], 1)
        self.assertEqual(counts_run_1["row_count"], 4)
        self.assertEqual(counts_run_1["has_row_count"], 4)

        # Run 2: Upload EXACT same CSV again
        ds_id_2, rows_2 = parse_and_validate_csv(self.employees_csv, "employees.csv")
        self.assertEqual(ds_id_1, ds_id_2, "Dataset ID must be identical for identical content")

        for idx, r in enumerate(rows_2):
            self.db.merge_row(ds_id_2, "employees.csv", idx, r, "2026-09-11T11:05:00Z")

        counts_run_2 = self.db.get_counts()

        # Compare first and second runs
        self.assertEqual(
            counts_run_1,
            counts_run_2,
            f"Counts must remain identical. Run 1: {counts_run_1}, Run 2: {counts_run_2}",
        )

    def test_different_csv_creates_new_nodes(self):
        """
        Tests that uploading a completely different CSV creates its own Dataset node
        and Row nodes without colliding.
        """
        # Ingest employees
        ds_id_emp, emp_rows = parse_and_validate_csv(self.employees_csv, "employees.csv")
        for idx, r in enumerate(emp_rows):
            self.db.merge_row(ds_id_emp, "employees.csv", idx, r, "2026-09-11T11:00:00Z")

        # Ingest products with completely different columns
        products_csv = (
            b"Product,Category,Price\n"
            b"Quantum Laptop,Electronics,1200\n"
            b"Desk Lamp,Office,40\n"
        )
        ds_id_prod, prod_rows = parse_and_validate_csv(products_csv, "products.csv")
        for idx, r in enumerate(prod_rows):
            self.db.merge_row(ds_id_prod, "products.csv", idx, r, "2026-09-11T11:01:00Z")

        counts = self.db.get_counts()
        self.assertEqual(counts["dataset_count"], 2)
        self.assertEqual(counts["row_count"], 6)  # 4 + 2
        self.assertEqual(counts["has_row_count"], 6)


if __name__ == "__main__":
    unittest.main()
