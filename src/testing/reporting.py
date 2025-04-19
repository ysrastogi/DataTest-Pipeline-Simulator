from typing import Dict, List, Any, Optional
import os
import json
import datetime
import xml.etree.ElementTree as ET
import xml.dom.minidom
import logging
from src.testing.core import TestCase, TestSuite

logger = logging.getLogger("testing.reporting")

class TestReporter:
    """Generates reports from test results."""
    
    def __init__(self, output_dir: str = "test_reports"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def generate_json_report(self, results: Dict[str, Any], 
                           filename: str = "test_results.json") -> str:
        """Generate a JSON report from test results."""
        output_path = os.path.join(self.output_dir, filename)
        
        # Add timestamp to results
        results["generated_at"] = datetime.datetime.now().isoformat()
        
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Generated JSON report: {output_path}")
        return output_path
    
    def generate_junit_xml(self, results: Dict[str, Any], 
                         filename: str = "junit_results.xml") -> str:
        """Generate a JUnit XML report from test results."""
        output_path = os.path.join(self.output_dir, filename)
        
        # Create the root element
        test_suites = ET.Element("testsuites")
        test_suites.set("name", "Pipeline Tests")
        test_suites.set("tests", str(results["total"]))
        test_suites.set("failures", str(results["failed"]))
        test_suites.set("errors", str(results["error"]))
        test_suites.set("skipped", str(results["skipped"]))
        test_suites.set("time", str(results.get("duration", 0)))
        
        # Create a testsuite element
        test_suite = ET.SubElement(test_suites, "testsuite")
        test_suite.set("name", "PipelineTestSuite")
        test_suite.set("tests", str(results["total"]))
        test_suite.set("failures", str(results["failed"]))
        test_suite.set("errors", str(results["error"]))
        test_suite.set("skipped", str(results["skipped"]))
        test_suite.set("time", str(results.get("duration", 0)))
        
        # Add test cases
        for test_result in results.get("test_results", []):
            test_case = ET.SubElement(test_suite, "testcase")
            test_case.set("name", test_result["name"])
            test_case.set("classname", "PipelineTest")
            test_case.set("time", str(test_result.get("duration", 0)))
            
            if test_result["status"] == "failed":
                failure = ET.SubElement(test_case, "failure")
                failure.set("message", test_result.get("error_message", "Test failed"))
                failure.text = test_result.get("error_details", "")
            
            elif test_result["status"] == "error":
                error = ET.SubElement(test_case, "error")
                error.set("message", test_result.get("error_message", "Test error"))
                error.text = test_result.get("error_details", "")
            
            elif test_result["status"] == "skipped":
                ET.SubElement(test_case, "skipped")
        
        # Write to file with pretty formatting
        xml_str = ET.tostring(test_suites, encoding="utf-8")
        dom = xml.dom.minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ")
        
        with open(output_path, 'w') as f:
            f.write(pretty_xml)
        
        logger.info(f"Generated JUnit XML report: {output_path}")
        return output_path
    
    def generate_html_report(self, results: Dict[str, Any], 
                           filename: str = "test_report.html") -> str:
        """Generate an HTML report from test results."""
        output_path = os.path.join(self.output_dir, filename)
        
        # Calculate pass rate
        total = results["total"]
        passed = results["passed"]
        pass_rate = (passed / total) * 100 if total > 0 else 0
        
        # Format timestamps
        start_time = results.get("start_time", 0)
        start_time_str = datetime.datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
        
        # Generate test results HTML
        test_rows = []
        for test_result in results.get("test_results", []):
            status_class = {
                "passed": "success",
                "failed": "danger",
                "error": "warning",
                "skipped": "secondary"
            }.get(test_result["status"], "")
            
            duration = test_result.get("duration", 0)
            duration_str = f"{duration:.2f}s" if duration is not None else "N/A"
            
            error_details = ""
            if test_result["status"] in ["failed", "error"]:
                error_message = test_result.get("error_message", "")
                error_details_text = test_result.get("error_details", "")
                
                error_details = f"""
                <div class="error-details">
                    <strong>Error:</strong> {error_message}
                    <pre>{error_details_text}</pre>
                </div>
                """
            
            row = f"""
            <tr class="{status_class}">
                <td>{test_result["name"]}</td>
                <td class="text-center">{test_result["status"].upper()}</td>
                <td class="text-center">{duration_str}</td>
                <td>{', '.join(test_result.get("tags", []))}</td>
                <td>{error_details}</td>
            </tr>
            """
            test_rows.append(row)
        
        # Generate HTML
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Pipeline Test Report</title>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
    <style>
        body {{ padding: 20px; }}
        .summary-card {{ margin-bottom: 20px; }}
        .test-details {{ margin-top: 30px; }}
        .error-details {{ margin-top: 10px; }}
        .error-details pre {{ max-height: 200px; overflow-y: auto; background-color: #f8f9fa; padding: 10px; }}
        .progress {{ height: 30px; margin-bottom: 20px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1 class="mb-4">Pipeline Test Report</h1>
        
        <div class="row summary-card">
            <div class="col-md-4">
                <div class="card">
                    <div class="card-header bg-primary text-white">Summary</div>
                    <div class="card-body">
                        <p><strong>Total Tests:</strong> {total}</p>
                        <p><strong>Passed:</strong> {passed}</p>
                        <p><strong>Failed:</strong> {results["failed"]}</p>
                        <p><strong>Errors:</strong> {results["error"]}</p>
                        <p><strong>Skipped:</strong> {results["skipped"]}</p>
                        <p><strong>Duration:</strong> {results.get("duration", 0):.2f}s</p>
                        <p><strong>Start Time:</strong> {start_time_str}</p>
                    </div>
                </div>
            </div>
            
            <div class="col-md-8">
                <div class="card">
                    <div class="card-header bg-primary text-white">Pass Rate</div>
                    <div class="card-body">
                        <div class="progress">
                            <div class="progress-bar bg-success" role="progressbar" style="width: {pass_rate}%;" 
                                 aria-valuenow="{pass_rate}" aria-valuemin="0" aria-valuemax="100">
                                {pass_rate:.1f}%
                            </div>
                        </div>
                        
                        <div class="row text-center">
                            <div class="col">
                                <div class="alert alert-success">
                                    <h4>{passed}</h4>
                                    <span>Passed</span>
                                </div>
                            </div>
                            <div class="col">
                                <div class="alert alert-danger">
                                    <h4>{results["failed"]}</h4>
                                    <span>Failed</span>
                                </div>
                            </div>
                            <div class="col">
                                <div class="alert alert-warning">
                                    <h4>{results["error"]}</h4>
                                    <span>Errors</span>
                                </div>
                            </div>
                            <div class="col">
                                <div class="alert alert-secondary">
                                    <h4>{results["skipped"]}</h4>
                                    <span>Skipped</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="test-details">
            <h2>Test Results</h2>
            <table class="table table-striped">
                <thead>
                    <tr>
                        <th>Test Name</th>
                        <th class="text-center">Status</th>
                        <th class="text-center">Duration</th>
                        <th>Tags</th>
                        <th>Details</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(test_rows)}
                </tbody>
            </table>
        </div>
    </div>
    
    <script src="https://code.jquery.com/jquery-3.5.1.slim.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/@popperjs/core@2.5.4/dist/umd/popper.min.js"></script>
    <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.min.js"></script>
</body>
</html>
"""
        
        with open(output_path, 'w') as f:
            f.write(html)
        
        logger.info(f"Generated HTML report: {output_path}")
        return output_path