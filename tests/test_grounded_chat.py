import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "api")))

from chat import GroundedChatEngine


class MockNeo4jDriverForChat:
    def __init__(self):
        self.mock_data = [
            {"Name": "Alice Johnson", "Department": "Engineering", "Salary": 95000, "City": "New York"},
            {"Name": "Bob Smith", "Department": "HR", "Salary": 62000, "City": "Chicago"},
            {"Name": "Charlie Brown", "Department": "Marketing", "Salary": 71000, "City": "San Francisco"},
            {"Name": "Diana Prince", "Department": "Engineering", "Salary": 105000, "City": "Seattle"},
            {"Name": "Evan Wright", "Department": "HR", "Salary": 58000, "City": "Chicago"},
            {"Name": "Julia Roberts", "Department": "HR", "Salary": 64000, "City": "Austin"},
        ]

    def session(self, database=None):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def run(self, cypher, params=None):
        params = params or {}
        # Health ping
        if "RETURN 1 AS ready" in cypher or "RETURN 1 AS ping" in cypher:
            return [{"ready": 1, "ping": 1}]

        # Count total rows
        if "RETURN count(r) AS cnt" in cypher or "RETURN count(r) AS total_rows" in cypher:
            return [{"cnt": len(self.mock_data), "total_rows": len(self.mock_data)}]

        # Dynamic keys
        if "UNWIND keys(r) AS k" in cypher:
            keys = ["Name", "Department", "Salary", "City"]
            return [{"k": k} for k in keys]

        # HR Department count
        if "WHERE toLower(toString(r.Department)) = $val" in cypher or "WHERE toLower(toString(r.Department)) = 'hr'" in cypher:
            val = params.get("val", "hr").lower()
            matching = [r for r in self.mock_data if r["Department"].lower() == val]
            return [{"count": len(matching)}]

        # Average salary
        if "avg(toInteger(r.Salary))" in cypher:
            avg_val = sum(r["Salary"] for r in self.mock_data) / len(self.mock_data)
            return [{"average_Salary": round(avg_val, 2)}]

        # Max salary
        if "max(toInteger(r.Salary))" in cypher:
            max_val = max(r["Salary"] for r in self.mock_data)
            return [{"max_Salary": max_val}]

        # Min salary
        if "min(toInteger(r.Salary))" in cypher:
            min_val = min(r["Salary"] for r in self.mock_data)
            return [{"min_Salary": min_val}]

        # Sum of salary
        if "sum(toInteger(r.Salary))" in cypher:
            sum_val = sum(r["Salary"] for r in self.mock_data)
            return [{"sum_Salary": sum_val}]

        # Distinct departments
        if "DISTINCT r.Department" in cypher:
            distinct_depts = sorted(list(set(r["Department"] for r in self.mock_data)))
            return [{"Department": d} for d in distinct_depts]

        # Name lookup (Alice)
        if "CONTAINS $term" in cypher:
            term = params.get("term", "").lower()
            matching = [r for r in self.mock_data if term in r["Name"].lower()]
            return [{"r": m} for m in matching]

        # Property of entity lookup
        if "RETURN r.Department AS Department" in cypher:
            term = params.get("term", "").lower()
            matching = [r for r in self.mock_data if term in r["Name"].lower()]
            if matching:
                return [{"Department": matching[0]["Department"], "r": matching[0]}]

        return []


class TestGroundedChat(unittest.TestCase):
    def setUp(self):
        self.engine = GroundedChatEngine()
        self.engine.get_driver = lambda: MockNeo4jDriverForChat()

    def test_8_actual_questions(self):
        """
        Executes and validates at least 8 distinct real questions as required by Phase 7/10.
        Verifies answer, cypher, result, and grounded=true.
        """
        questions = [
            # Q1: Filtered count
            ("How many employees are in HR?", True),
            # Q2: Aggregation - average
            ("What is the average salary?", True),
            # Q3: Aggregation - max
            ("What is the highest salary?", True),
            # Q4: Aggregation - min
            ("What is the lowest salary?", True),
            # Q5: Aggregation - sum
            ("What is the total sum of salary?", True),
            # Q6: Total rows
            ("How many total rows?", True),
            # Q7: Distinct listing
            ("List all departments", True),
            # Q8: Entity lookup
            ("Tell me about Alice Johnson", True),
            # Q9: Property of entity (e.g. user question: give me the department of employee 4)
            ("give me the department of Alice Johnson", True),
            # Q10: Property of entity 2
            ("what is the department of Bob Smith", True),
        ]

        self.assertGreaterEqual(len(questions), 8, "Must test at least 8 questions")

        for idx, (q, expected_grounded) in enumerate(questions, start=1):
            with self.subTest(q=q):
                resp = self.engine.process_query(q)
                # Contract verification
                self.assertIn("answer", resp)
                self.assertIn("cypher", resp)
                self.assertIn("result", resp)
                self.assertIn("grounded", resp)

                # Grounding verification
                self.assertEqual(resp["grounded"], expected_grounded, f"Question '{q}' should have grounded={expected_grounded}")
                self.assertTrue(len(resp["answer"]) > 0)
                self.assertNotEqual(resp["cypher"], "NONE")
                self.assertTrue(len(resp["result"]) > 0)
                print(f"[TEST PASS] Q{idx}: '{q}' -> Grounded: {resp['grounded']}, Cypher: {resp['cypher']}")

    def test_unsupported_questions_honestly_ungrounded(self):
        """
        Verifies that unsupported or out-of-domain questions return grounded=false
        and an explicit honest no-data statement without hallucinations.
        """
        unsupported = [
            "What is the capital of France?",
            "What is the weather in Tokyo right now?",
            "Who won the 1994 World Cup?",
        ]

        for q in unsupported:
            with self.subTest(q=q):
                resp = self.engine.process_query(q)
                self.assertFalse(resp["grounded"], f"Question '{q}' must NOT be grounded")
                self.assertEqual(resp["cypher"], "NONE")
                self.assertEqual(resp["result"], [])
                self.assertIn("do not have data in the knowledge graph", resp["answer"])


if __name__ == "__main__":
    unittest.main()
