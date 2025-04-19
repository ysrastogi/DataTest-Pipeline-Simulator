import streamlit as st
from dashboard.utils.data_loader import DataLoader

def show_home():
    """Display the home page."""
    st.title("DataTest Pipeline Simulator Dashboard")
    
    st.markdown("""
    Welcome to the DataTest Pipeline Simulator Dashboard. This application helps you visualize and manage your data pipelines, test results, and performance metrics.
    
    ### Features
    
    - **Pipeline Visualization**: View your pipeline flows and execution details
    - **Test Results**: Analyze test results and coverage
    - **Performance Metrics**: Monitor pipeline performance and resource usage
    
    ### Getting Started
    
    Use the sidebar to navigate to different sections of the dashboard.
    """)
    
    # Display pipeline summary
    st.header("Pipeline Summary")
    
    data_loader = DataLoader()
    pipeline_names = data_loader.get_available_pipelines()
    
    if pipeline_names:
        col1, col2 = st.columns(2)
        col1.metric("Total Pipelines", len(pipeline_names))
        
        # Display a list of available pipelines
        st.subheader("Available Pipelines")
        for name in pipeline_names:
            st.write(f"- {name}")
    else:
        st.info("No pipelines found. Create a pipeline to get started.")
        
        # Add a button to navigate to the pipelines page
        if st.button("Create Pipeline"):
            # This would normally set a session state variable to navigate
            # For simplicity, we'll just show a message
            st.success("Navigate to the Pipelines page using the sidebar.")