import streamlit as st
import streamlit.components.v1 as components
import os
import re
from typing import Optional

def render_mermaid_from_html(html_path: str) -> Optional[str]:
    """Extract Mermaid diagram code from an HTML file."""
    if not os.path.exists(html_path):
        return None
    
    with open(html_path, 'r') as f:
        html_content = f.read()
    
    # Extract the Mermaid diagram content using regex
    mermaid_match = re.search(r'<div class="mermaid">(.*?)</div>', html_content, re.DOTALL)
    if mermaid_match:
        return mermaid_match.group(1).strip()
    
    return None

def display_pipeline_flow(pipeline_name: str, flow_path: str):
    """Display a pipeline flow diagram."""
    st.subheader(f"Pipeline Flow: {pipeline_name}")
    
    if not os.path.exists(flow_path):
        st.error(f"Pipeline flow file not found: {flow_path}")
        return
    
    # Option 1: Display the HTML file directly
    with open(flow_path, 'r') as f:
        html_content = f.read()
        components.html(html_content, height=600)
    
    # Option 2: Extract and display the Mermaid diagram
    # mermaid_code = render_mermaid_from_html(flow_path)
    # if mermaid_code:
    #     st.markdown(f"```mermaid\n{mermaid_code}\n```")
    # else:
    #     st.error("Could not extract Mermaid diagram from the HTML file.")

def display_pipeline_dashboard(pipeline_name: str, dashboard_path: str):
    """Display a pipeline dashboard."""
    st.subheader(f"Pipeline Dashboard: {pipeline_name}")
    
    if not os.path.exists(dashboard_path):
        st.error(f"Dashboard file not found: {dashboard_path}")
        return
    
    # Display the HTML dashboard
    with open(dashboard_path, 'r') as f:
        html_content = f.read()
        components.html(html_content, height=800, scrolling=True)