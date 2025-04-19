from typing import Dict, List, Any, Optional, Callable, Union, Set
import os
import importlib.util
import inspect
import glob
import re
import logging
from src.testing.core import TestCase, TestSuite

logger = logging.getLogger("testing.discovery")

class TestCollector:
    """Discovers and collects test cases."""
    
    def __init__(self, test_dirs: Optional[List[str]] = None, 
                pattern: str = "*_test.py"):
        self.test_dirs = test_dirs or ["tests"]
        self.pattern = pattern
        self.discovered_tests: List[TestCase] = []
        self.discovered_suites: List[TestSuite] = []
    
    def discover_tests(self) -> 'TestCollector':
        """Discover test cases in the specified directories."""
        logger.info(f"Discovering tests in directories: {self.test_dirs}")
        
        for test_dir in self.test_dirs:
            if not os.path.exists(test_dir):
                logger.warning(f"Test directory does not exist: {test_dir}")
                continue
            
            # Find all Python files matching the pattern
            pattern_path = os.path.join(test_dir, "**", self.pattern)
            test_files = glob.glob(pattern_path, recursive=True)
            
            logger.info(f"Found {len(test_files)} test files in {test_dir}")
            
            for test_file in test_files:
                self._process_test_file(test_file)
        
        logger.info(f"Total discovered: {len(self.discovered_tests)} test cases, " +
                  f"{len(self.discovered_suites)} test suites")
        
        return self
    
    def _process_test_file(self, file_path: str) -> None:
        """Process a test file to extract test cases and suites."""
        logger.debug(f"Processing test file: {file_path}")
        
        try:
            # Load the module
            module_name = os.path.basename(file_path).replace(".py", "")
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Find all test cases and test suites in the module
            for name, obj in inspect.getmembers(module):
                if isinstance(obj, TestCase):
                    self.discovered_tests.append(obj)
                elif isinstance(obj, TestSuite):
                    self.discovered_suites.append(obj)
        
        except Exception as e:
            logger.error(f"Error processing test file {file_path}: {str(e)}")
    
    def create_suite_from_discovered(self, name: str, filter_func: Optional[Callable[[TestCase], bool]] = None) -> TestSuite:
        """Create a test suite from discovered test cases, optionally filtered."""
        suite = TestSuite(name)
        
        for test_case in self.discovered_tests:
            if filter_func is None or filter_func(test_case):
                suite.add_test_case(test_case)
        
        return suite
    
    def filter_by_tag(self, tag: str) -> List[TestCase]:
        """Filter discovered test cases by tag."""
        return [tc for tc in self.discovered_tests if tag in tc.tags]
    
    def filter_by_name_pattern(self, pattern: str) -> List[TestCase]:
        """Filter discovered test cases by name pattern."""
        regex = re.compile(pattern)
        return [tc for tc in self.discovered_tests if regex.search(tc.name)]

class TestRegistry:
    """Registry for test cases and suites."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TestRegistry, cls).__new__(cls)
            cls._instance.test_cases = {}
            cls._instance.test_suites = {}
        return cls._instance
    
    def register_test_case(self, test_case: TestCase) -> None:
        """Register a test case."""
        self.test_cases[test_case.id] = test_case
    
    def register_test_suite(self, test_suite: TestSuite) -> None:
        """Register a test suite."""
        self.test_suites[test_suite.id] = test_suite
    
    def get_test_case(self, id: str) -> Optional[TestCase]:
        """Get a test case by ID."""
        return self.test_cases.get(id)
    
    def get_test_suite(self, id: str) -> Optional[TestSuite]:
        """Get a test suite by ID."""
        return self.test_suites.get(id)
    
    def get_all_test_cases(self) -> List[TestCase]:
        """Get all registered test cases."""
        return list(self.test_cases.values())
    
    def get_all_test_suites(self) -> List[TestSuite]:
        """Get all registered test suites."""
        return list(self.test_suites.values())