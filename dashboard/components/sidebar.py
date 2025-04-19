import streamlit as st

def render_sidebar():
    """Render the sidebar navigation and return the selected page."""
    
    st.sidebar.title("DataTest Pipeline Simulator")
    
    # Add logo or image
    st.sidebar.image("https://via.placeholder.com/150x150.png?text=DT", width=150)
    
    # Navigation
    st.sidebar.header("Navigation")
    page = st.sidebar.radio(
        "Go to",
        options=["Home", "Pipelines", "Tests", "Metrics"],
        index=0
    )
    
    # Add information about the app
    st.sidebar.markdown("---")
    st.sidebar.info(
        "This dashboard provides visualization and management "
        "capabilities for data pipelines and their test results."
    )
    
    # Add version information
    st.sidebar.markdown("---")
    st.sidebar.caption("Version 0.1.0")
    
    return page