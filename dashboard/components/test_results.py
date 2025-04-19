import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import os
from typing import List, Dict, Any, Optional

def display_test_report(report_path: str):
    """Display a test report HTML file."""
    st.subheader("Test Report")
    
    if not os.path.exists(report_path):
        st.error(f"Test report file not found: {report_path}")
        return
    
    # Display the HTML report
    with open(report_path, 'r') as f:
        html_content = f.read()
        components.html(html_content, height=800, scrolling=True)

def display_test_results_table(test_results: pd.DataFrame):
    """Display test results as a table."""
    st.subheader("Test Results")
    
    if test_results.empty:
        st.warning("No test results available.")
        return
    
    # Add styling to the DataFrame
    def highlight_status(val):
        if val == "Passed":
            return "background-color: #90EE90"  # Light green
        elif val == "Failed":
            return "background-color: #FFCCCB"  # Light red
        return ""
    
    styled_df = test_results.style.apply(
        lambda row: [highlight_status(row["status"])] * len(row),
        axis=1
    )
    
    # Display the styled table
    st.dataframe(styled_df)
    
    # Display summary statistics
    passed = test_results[test_results["status"] == "Passed"].shape[0]
    failed = test_results[test_results["status"] == "Failed"].shape[0]
    total = test_results.shape[0]
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Tests", total)
    col2.metric("Passed", passed)
    col3.metric("Failed", failed)

def display_test_summary(test_results: pd.DataFrame):
    """Display a summary of test results by pipeline."""
    if test_results.empty:
        return
    
    st.subheader("Test Summary by Pipeline")
    
    # Group by pipeline and count tests by status
    summary = test_results.groupby(["pipeline", "status"]).size().unstack().fillna(0)
    
    # Calculate pass rate
    if "Passed" in summary.columns:
        summary["Pass Rate"] = summary["Passed"] / summary.sum(axis=1) * 100
    
    # Display the summary
    st.dataframe(summary)