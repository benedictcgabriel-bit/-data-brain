import io
import os
import sys
import time
import unittest

# Add project root to sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(BASE_DIR, "api"))
sys.path.insert(0, os.path.join(BASE_DIR, "loader"))
sys.path.insert(0, os.path.join(BASE_DIR, "tests"))

from test_api_contracts import TestApiContracts
from test_hostile_inputs import TestHostileInputs
from test_idempotency import TestIdempotency
from test_grounded_chat import TestGroundedChat
from test_health_readiness import TestHealthReadiness


def run_master_test_suite():
    print("=" * 80)
    print("RUNNING MASTER TEST SUITE: CSV -> Kafka -> Neo4j Grounded Chatbot App")
    print("=" * 80)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    test_classes = [
        TestApiContracts,
        TestHostileInputs,
        TestIdempotency,
        TestGroundedChat,
        TestHealthReadiness,
    ]

    for tc in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(tc))

    start_time = time.time()
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    elapsed = time.time() - start_time

    total_tests = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    passed = total_tests - (failures + errors)

    print("\n" + "=" * 80)
    print("TEST EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Total Tests Executed : {total_tests}")
    print(f"Total Tests Passed   : {passed}")
    print(f"Total Failures       : {failures}")
    print(f"Total Errors         : {errors}")
    print(f"Execution Duration   : {elapsed:.3f} seconds")
    print(f"Overall Test Status  : {'ALL PASSED (100% SUCCESS)' if (failures == 0 and errors == 0) else 'FAILURES DETECTED'}")
    print("=" * 80)

    return failures == 0 and errors == 0


if __name__ == "__main__":
    success = run_master_test_suite()
    sys.exit(0 if success else 1)
