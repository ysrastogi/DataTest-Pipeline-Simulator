import streamlit as st
import os
import sys

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import components
from dashboard.components.sidebar import render_sidebar
from dashboard.pages.home import show_home
from dashboard.pages.pipelines import show_pipelines
from dashboard.pages.tests import show_tests
from dashboard.pages.metrics import show_metrics

# Configure the app
st.set_page_config(
    page_title="DataTest Pipeline Simulator",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    # Render the sidebar for navigation
    page = render_sidebar()
    
    # Display the selected page
    if page == "Home":
        show_home()
    elif page == "Pipelines":
        show_pipelines()
    elif page == "Tests":
        show_tests()
    elif page == "Metrics":
        show_metrics()

if __name__ == "__main__":
    main()