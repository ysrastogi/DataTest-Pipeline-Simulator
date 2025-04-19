import streamlit as st
import os
from dashboard.utils.data_loader import DataLoader
from dashboard.components.test_results import display_test_results_table, display_test_summary, display_test_report

def show_tests():
    """Display the test results page."""
    st.title("Test Results")
    
    data_loader = DataLoader()
    test_results = data_loader.get_test_results()
    
    # Display test results table
    display_test_results_table(test_results)
    
    # Display test summary by pipeline
    display_test_summary(test_results)
    
    # Allow viewing specific test reports
    st.header("Test Reports")
    
    # Look for available test reports
    report_files = [
        f for f in os.listdir(data_loader.reports_dir) 
        if f.endswith("_tests_report.html")
    ]
    
    if report_files:
        selected_report = st.selectbox(
            "Select a test report to view",
            options=report_files
        )
        
        if selected_report:
            report_path = os.path.join(data_loader.reports_dir, selected_report)
            display_test_report(report_path)
    else:
        st.info("No test reports found.")