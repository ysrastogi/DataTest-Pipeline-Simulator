import streamlit as st
from dashboard.utils.data_loader import DataLoader
from dashboard.components.pipeline_viewer import display_pipeline_flow, display_pipeline_dashboard

def show_pipelines():
    """Display the pipelines management page."""
    st.title("Data Pipelines")
    
    data_loader = DataLoader()
    pipeline_names = data_loader.get_available_pipelines()
    
    if not pipeline_names:
        st.info("No pipelines found. Create a pipeline to get started.")
        return
    
    # Pipeline selector
    selected_pipeline = st.selectbox(
        "Select a pipeline to view",
        options=pipeline_names
    )
    
    if selected_pipeline:
        # Get pipeline data
        pipeline_data = data_loader.get_pipeline_data(selected_pipeline)
        
        # Display pipeline information
        st.header(f"Pipeline: {selected_pipeline}")
        
        # Tabs for different views
        tab1, tab2 = st.tabs(["Flow Diagram", "Dashboard"])
        
        with tab1:
            if pipeline_data.get("flow_path"):
                display_pipeline_flow(
                    pipeline_name=selected_pipeline, 
                    flow_path=pipeline_data["flow_path"]
                )
            else:
                st.warning("No flow diagram available for this pipeline.")
        
        with tab2:
            if pipeline_data.get("dashboard_path"):
                display_pipeline_dashboard(
                    pipeline_name=selected_pipeline,
                    dashboard_path=pipeline_data["dashboard_path"]
                )
            else:
                st.warning("No dashboard available for this pipeline.")