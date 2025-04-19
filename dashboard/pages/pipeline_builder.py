import streamlit as st
import json
import os
from typing import Dict, List, Any
from streamlit_elements import elements, dashboard, mui, html
import networkx as nx
import uuid

def show_pipeline_builder():
    """Display the interactive pipeline builder page."""
    st.title("Interactive Pipeline Builder")
    
    # Initialize session state for storing the pipeline configuration
    if "pipeline_nodes" not in st.session_state:
        st.session_state.pipeline_nodes = []
    if "pipeline_edges" not in st.session_state:
        st.session_state.pipeline_edges = []
    if "selected_node" not in st.session_state:
        st.session_state.selected_node = None
    
    # Create a two-column layout
    col1, col2 = st.columns([1, 3])
    
    with col1:
        st.subheader("Components")
        
        # Component categories
        with st.expander("Data Sources", expanded=True):
            if st.button("CSV Source"):
                add_node("csv_source", "CSV Source", {"file_path": "", "delimiter": ","})
            if st.button("Database Source"):
                add_node("db_source", "Database Source", {"connection_string": "", "query": ""})
            if st.button("API Source"):
                add_node("api_source", "API Source", {"url": "", "method": "GET"})
        
        with st.expander("Transformations", expanded=True):
            if st.button("Filter"):
                add_node("filter", "Filter", {"condition": ""})
            if st.button("Join"):
                add_node("join", "Join", {"join_type": "inner", "on": ""})
            if st.button("Aggregate"):
                add_node("aggregate", "Aggregate", {"group_by": "", "aggregations": {}})
        
        with st.expander("Destinations", expanded=True):
            if st.button("CSV Destination"):
                add_node("csv_dest", "CSV Destination", {"file_path": "", "mode": "overwrite"})
            if st.button("Database Destination"):
                add_node("db_dest", "Database Destination", {"connection_string": "", "table": ""})
        
        # Save pipeline button
        if st.button("Save Pipeline"):
            save_pipeline()
        
        # Load pipeline selection
        pipeline_files = get_saved_pipelines()
        if pipeline_files:
            selected_pipeline = st.selectbox("Load Saved Pipeline", ["Select..."] + pipeline_files)
            if selected_pipeline != "Select..." and st.button("Load"):
                load_pipeline(selected_pipeline)
    
    with col2:
        st.subheader("Pipeline Design")
        
        # Using streamlit_elements for the interactive graph
        with elements("pipeline_graph"):
            # Create dashboard layout
            layout = [
                # Define the layout for the graph component
                dashboard.Item("graph", 0, 0, 12, 6, isDraggable=True, isResizable=True)
            ]
            
            with dashboard.Grid(layout=layout, draggableHandle=".draggable"):
                with mui.Paper(key="graph", sx={"display": "flex", "flexDirection": "column", "height": 600}):
                    mui.Typography("Pipeline Flow", className="draggable", sx={"padding": "10px", "backgroundColor": "#f0f0f0"})
                    render_pipeline_graph()
        
        # Node configuration panel (appears when a node is selected)
        if st.session_state.selected_node is not None:
            node = next((n for n in st.session_state.pipeline_nodes if n["id"] == st.session_state.selected_node), None)
            if node:
                st.subheader(f"Configure: {node['label']}")
                
                # Create input fields for each property in the node config
                updated_config = {}
                for key, value in node["config"].items():
                    if isinstance(value, str):
                        updated_config[key] = st.text_input(key.replace("_", " ").title(), value)
                    elif isinstance(value, bool):
                        updated_config[key] = st.checkbox(key.replace("_", " ").title(), value)
                    elif isinstance(value, (int, float)):
                        updated_config[key] = st.number_input(key.replace("_", " ").title(), value=value)
                    elif isinstance(value, dict):
                        st.write(f"{key.replace('_', ' ').title()}:")
                        updated_config[key] = value  # Just keep the original for complex objects
                
                # Update node button
                if st.button("Update Node"):
                    update_node_config(node["id"], updated_config)
                    st.success(f"Updated {node['label']} configuration")
                
                # Delete node button
                if st.button("Delete Node", type="primary"):
                    delete_node(node["id"])
                    st.session_state.selected_node = None
                    st.rerun()

def add_node(node_type: str, label: str, config: Dict[str, Any]):
    """Add a new node to the pipeline."""
    node_id = str(uuid.uuid4())
    st.session_state.pipeline_nodes.append({
        "id": node_id,
        "type": node_type,
        "label": label,
        "config": config
    })
    st.rerun()

def update_node_config(node_id: str, new_config: Dict[str, Any]):
    """Update the configuration of a node."""
    for node in st.session_state.pipeline_nodes:
        if node["id"] == node_id:
            node["config"] = new_config
            break

def delete_node(node_id: str):
    """Delete a node and its connections from the pipeline."""
    # Remove the node
    st.session_state.pipeline_nodes = [
        node for node in st.session_state.pipeline_nodes if node["id"] != node_id
    ]
    
    # Remove any edges connected to this node
    st.session_state.pipeline_edges = [
        edge for edge in st.session_state.pipeline_edges 
        if edge["source"] != node_id and edge["target"] != node_id
    ]

def add_edge(source_id: str, target_id: str):
    """Add a new edge between two nodes."""
    # Check if edge already exists
    for edge in st.session_state.pipeline_edges:
        if edge["source"] == source_id and edge["target"] == target_id:
            return
    
    # Add the new edge
    st.session_state.pipeline_edges.append({
        "id": f"{source_id}-{target_id}",
        "source": source_id,
        "target": target_id
    })

def render_pipeline_graph():
    """Render the interactive pipeline graph using react-flow or another library."""
    # Here we'd typically use a JavaScript library like React Flow
    # Since we're using streamlit-elements, we can use Material UI components
    # to create a simplified representation
    
    # For a real implementation, you'd integrate with a JS graph library
    # This is a placeholder that shows node information
    
    for node in st.session_state.pipeline_nodes:
        with mui.Card(sx={"margin": "10px", "padding": "10px"}):
            mui.Typography(node["label"], variant="h6")
            mui.Typography(f"Type: {node['type']}")
            
            # Show connections
            connected_to = [
                next((n["label"] for n in st.session_state.pipeline_nodes if n["id"] == edge["target"]), "Unknown")
                for edge in st.session_state.pipeline_edges
                if edge["source"] == node["id"]
            ]
            
            if connected_to:
                mui.Typography("Connected to: " + ", ".join(connected_to))
            
            # Button to select this node for editing
            if mui.Button("Edit", onClick=lambda id=node["id"]: select_node(id)):
                pass
            
            # This would be replaced with actual graph rendering in a production app

def select_node(node_id):
    """Select a node for editing."""
    st.session_state.selected_node = node_id

def save_pipeline():
    """Save the current pipeline configuration to a file."""
    pipeline_name = st.text_input("Pipeline Name")
    if not pipeline_name:
        st.warning("Please enter a pipeline name")
        return
    
    pipeline_data = {
        "name": pipeline_name,
        "nodes": st.session_state.pipeline_nodes,
        "edges": st.session_state.pipeline_edges
    }
    
    # Create directory if it doesn't exist
    os.makedirs("pipelines", exist_ok=True)
    
    # Save to file
    with open(f"pipelines/{pipeline_name}.json", "w") as f:
        json.dump(pipeline_data, f, indent=2)
    
    st.success(f"Pipeline '{pipeline_name}' saved successfully")

def load_pipeline(pipeline_name: str):
    """Load a pipeline configuration from a file."""
    try:
        with open(f"pipelines/{pipeline_name}", "r") as f:
            pipeline_data = json.load(f)
        
        st.session_state.pipeline_nodes = pipeline_data["nodes"]
        st.session_state.pipeline_edges = pipeline_data["edges"]
        st.success(f"Pipeline '{pipeline_name}' loaded successfully")
    except Exception as e:
        st.error(f"Error loading pipeline: {str(e)}")

def get_saved_pipelines() -> List[str]:
    """Get a list of saved pipeline files."""
    if not os.path.exists("pipelines"):
        return []
    
    return [f for f in os.listdir("pipelines") if f.endswith(".json")]