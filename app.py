import streamlit as st
import os
import sys

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import components
from dashboard.pages.home import show_home
from dashboard.pages.pipelines import show_pipelines
from dashboard.pages.tests import show_tests
from dashboard.pages.metrics import show_metrics
from dashboard.pages.data_quality import show_data_quality
from dashboard.pages.pipeline_builder import show_pipeline_builder
from dashboard.pages.report_generator import show_report_generator

# Configure the app
st.set_page_config(
    page_title="DataTest Pipeline Simulator",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    """Main application entry point."""
    
    # Sidebar
    st.sidebar.title("DataTest Pipeline Simulator")
    
    # Add logo or image
    st.sidebar.image("https://via.placeholder.com/150x150.png?text=DT", width=150)
    
    # Navigation
    st.sidebar.header("Navigation")
    page = st.sidebar.radio(
        "Go to",
        options=[
            "Home",
            "Pipelines",
            "Pipeline Builder",
            "Tests",
            "Performance Metrics",
            "Data Quality",
            "Report Generator"
        ],
        index=0
    )
    
    # Render the selected page
    if page == "Home":
        show_home()
    elif page == "Pipelines":
        show_pipelines()
    elif page == "Pipeline Builder":
        show_pipeline_builder()
    elif page == "Tests":
        show_tests()
    elif page == "Performance Metrics":
        show_metrics()
    elif page == "Data Quality":
        show_data_quality()
    elif page == "Report Generator":
        show_report_generator()
    
    # Add information about the app
    st.sidebar.markdown("---")
    st.sidebar.info(
        "This dashboard provides advanced visualization and management "
        "capabilities for data pipelines, tests, and quality metrics."
    )
    
    # Add version information
    st.sidebar.markdown("---")
    st.sidebar.caption("Version 0.2.0")

if __name__ == "__main__":
    main()