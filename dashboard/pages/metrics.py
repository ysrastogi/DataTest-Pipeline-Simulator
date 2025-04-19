import streamlit as st
import os
import streamlit.components.v1 as components
import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Dict, Any, Optional
from dashboard.utils.data_loader import DataLoader

def display_performance_dashboard(dashboard_path: Optional[str] = None):
    """Display the performance dashboard."""
    st.subheader("Performance Dashboard")
    
    if dashboard_path and os.path.exists(dashboard_path):
        with open(dashboard_path, 'r') as f:
            html_content = f.read()
            components.html(html_content, height=800, scrolling=True)
    else:
        st.warning("No performance dashboard available.")

def display_benchmark_results(benchmark_results: List[Dict[str, Any]]):
    """Display benchmark results as a bar chart."""
    if not benchmark_results:
        st.warning("No benchmark results available.")
        return
    
    # Create a DataFrame from the benchmark results
    df = pd.DataFrame(benchmark_results)
    
    # Display a bar chart of durations
    st.subheader("Benchmark Durations")
    fig, ax = plt.subplots(figsize=(10, 6))
    df.plot.bar(x='name', y='avg_duration', ax=ax)
    ax.set_ylabel('Duration (s)')
    ax.set_title('Average Execution Duration by Benchmark')
    st.pyplot(fig)
    
    # Display the raw data in a table
    st.subheader("Benchmark Results")
    st.dataframe(df)

def display_pipeline_metrics(metrics_list: List[Dict[str, Any]]):
    """Display pipeline performance metrics."""
    if not metrics_list:
        st.warning("No pipeline metrics available.")
        return
    
    # Create a DataFrame from the metrics
    df = pd.DataFrame(metrics_list)
    
    # Display a bar chart of durations
    st.subheader("Pipeline Durations")
    fig, ax = plt.subplots(figsize=(10, 6))
    df.plot.bar(x='pipeline_name', y='total_duration_ms', ax=ax)
    ax.set_ylabel('Duration (ms)')
    ax.set_title('Total Execution Duration by Pipeline')
    st.pyplot(fig)
    
    # Display the raw data in a table
    st.subheader("Pipeline Metrics")
    st.dataframe(df)

def show_metrics():
    """Display the performance metrics page."""
    st.title("Performance Metrics")
    
    data_loader = DataLoader()
    
    # Option to view the complete dashboard
    st.header("Performance Dashboard")
    
    dashboard_path = data_loader.load_performance_dashboard()
    if dashboard_path:
        display_performance_dashboard(dashboard_path)
    else:
        st.info("No performance dashboard available.")
        
        # Offer to generate a dashboard
        if st.button("Generate Performance Dashboard"):
            st.warning("This functionality requires API integration.")
    
    # Display individual performance charts
    st.header("Performance Charts")
    
    # Look for available performance charts
    charts_dir = os.path.join(data_loader.charts_dir)
    if os.path.exists(charts_dir):
        chart_files = [
            f for f in os.listdir(charts_dir) 
            if f.endswith(".png") and f.startswith("pipeline_") or f.startswith("stage_")
        ]
        
        if chart_files:
            for chart_file in chart_files:
                chart_path = os.path.join(charts_dir, chart_file)
                st.image(chart_path, caption=chart_file.replace("_", " ").replace(".png", ""))
        else:
            st.info("No performance charts found.")
    else:
        st.info("Performance charts directory not found.")