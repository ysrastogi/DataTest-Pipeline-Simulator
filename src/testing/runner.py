from typing import Dict, List, Any, Optional, Union
from pyspark.sql import SparkSession
import argparse
import logging
import os
import time
import sys
from src.testing.core import TestCase, TestSuite
from src.testing.discovery import TestCollector
from src.testing.reporting import TestReporter

logger = logging.getLogger("testing.runner")

class TestRunner:
    """Runs tests and generates reports."""
    
    def __init__(self, spark: Optional[SparkSession] = None, 
                output_dir: str = "test_reports"):
        self.spark = spark or SparkSession.builder.getOrCreate()
        self.output_dir = output_dir
        self.reporter = TestReporter(output_dir)
    
    def run_suite(self, suite: TestSuite) -> Dict[str, Any]:
        """Run a test suite and return the results."""
        logger.info(f"Running test suite: {suite.name}")
        
        # Run the suite
        results = suite.run(spark=self.spark)
        
        # Generate reports
        self.reporter.generate_json_report(results, f"{suite.name.lower().replace(' ', '_')}_results.json")
        self.reporter.generate_html_report(results, f"{suite.name.lower().replace(' ', '_')}_report.html")
        self.reporter.generate_junit_xml(results, f"{suite.name.lower().replace(' ', '_')}_junit.xml")
        
        return results
    
    def run_test(self, test: TestCase) -> Dict[str, Any]:
        """Run a single test and return the result."""
        logger.info(f"Running test: {test.name}")
        
        # Create a single-test suite
        suite = TestSuite(f"Single Test: {test.name}")
        suite.add_test_case(test)
        
        return self.run_suite(suite)
    
    def discover_and_run(self, test_dirs: Optional[List[str]] = None, 
                       pattern: str = "*_test.py",
                       tags: Optional[List[str]] = None) -> Dict[str, Any]:
        """Discover tests and run them."""
        # Discover tests
        collector = TestCollector(test_dirs, pattern)
        collector.discover_tests()
        
        # Filter by tags if specified
        discovered_tests = collector.discovered_tests
        if tags:
            filtered_tests = []
            for test in discovered_tests:
                if any(tag in test.tags for tag in tags):
                    filtered_tests.append(test)
            discovered_tests = filtered_tests
        
        # Create a suite from the discovered tests
        suite = TestSuite("Discovered Tests")
        for test in discovered_tests:
            suite.add_test_case(test)
        
        # Run the suite
        return self.run_suite(suite)

def main():
    """Command-line entry point for the test runner."""
    parser = argparse.ArgumentParser(description="Run pipeline tests")
    parser.add_argument("--test-dirs", nargs="+", default=["tests"], 
                      help="Directories to search for tests")
    parser.add_argument("--pattern", default="*_test.py", 
                      help="Pattern for test file discovery")
    parser.add_argument("--tags", nargs="+", 
                      help="Only run tests with these tags")
    parser.add_argument("--output-dir", default="test_reports", 
                      help="Directory for test reports")
    parser.add_argument("--log-level", default="INFO", 
                      choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                      help="Logging level")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Create the test runner
    runner = TestRunner(output_dir=args.output_dir)
    
    # Run tests
    try:
        results = runner.discover_and_run(
            test_dirs=args.test_dirs,
            pattern=args.pattern,
            tags=args.tags
        )
        
        # Print summary
        total = results["total"]
        passed = results["passed"]
        failed = results["failed"]
        errors = results["error"]
        skipped = results["skipped"]
        
        print("\n" + "=" * 80)
        print(f"TEST SUMMARY: {passed}/{total} passed")
        print(f"  Passed:  {passed}")
        print(f"  Failed:  {failed}")
        print(f"  Errors:  {errors}")
        print(f"  Skipped: {skipped}")
        print("=" * 80 + "\n")
        
        # Exit with non-zero code if there are failures or errors
        if failed > 0 or errors > 0:
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Error running tests: {str(e)}", exc_info=True)
        sys.exit(2)

if __name__ == "__main__":
    main()