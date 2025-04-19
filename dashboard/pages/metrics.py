import streamlit as st
import os
import streamlit.components.v1 as components
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import numpy as np
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
    fig = px.bar(df, x='name', y='avg_duration', 
                 labels={'name': 'Benchmark', 'avg_duration': 'Average Duration (s)'},
                 title='Average Execution Duration by Benchmark',
                 color='avg_duration',
                 color_continuous_scale='Viridis')
    st.plotly_chart(fig, use_container_width=True)
    
    # Display the raw data in a table
    st.subheader("Benchmark Results")
    st.dataframe(df, use_container_width=True)

def display_pipeline_metrics(metrics_list: List[Dict[str, Any]]):
    """Display pipeline performance metrics."""
    if not metrics_list:
        st.warning("No pipeline metrics available.")
        return
    
    # Create a DataFrame from the metrics
    df = pd.DataFrame(metrics_list)
    
    # Display a bar chart of durations
    st.subheader("Pipeline Durations")
    fig = px.bar(df, x='pipeline_name', y='total_duration_ms',
                 labels={'pipeline_name': 'Pipeline', 'total_duration_ms': 'Duration (ms)'},
                 title='Total Execution Duration by Pipeline',
                 color='total_duration_ms',
                 color_continuous_scale='Viridis')
    st.plotly_chart(fig, use_container_width=True)
    
    # Display the raw data in a table
    st.subheader("Pipeline Metrics")
    st.dataframe(df, use_container_width=True)

def display_resource_usage(resource_data: Optional[pd.DataFrame] = None):
    """Display resource usage metrics over time."""
    st.subheader("Resource Usage")
    
    # If no data is provided, generate sample data for demonstration
    if resource_data is None:
        # Generate sample data for the last 24 hours with 15-min intervals
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)
        timestamps = pd.date_range(start=start_time, end=end_time, freq='15T')
        
        # Generate CPU, memory, and I/O data with some patterns
        cpu_usage = np.clip(
            [30 + 20 * np.sin(i / 10) + np.random.normal(0, 5) for i in range(len(timestamps))],
            0, 100
        )
        memory_usage = np.clip(
            [40 + 10 * np.sin(i / 20) + np.random.normal(0, 3) for i in range(len(timestamps))],
            0, 100
        )
        io_usage = np.clip(
            [20 + 15 * np.sin(i / 15) + np.random.normal(0, 7) for i in range(len(timestamps))],
            0, 100
        )
        
        resource_data = pd.DataFrame({
            'timestamp': timestamps,
            'cpu_pct': cpu_usage,
            'memory_pct': memory_usage,
            'io_pct': io_usage
        })
    
    # Create time series visualization with Plotly
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=resource_data['timestamp'],
        y=resource_data['cpu_pct'],
        mode='lines',
        name='CPU Usage (%)'
    ))
    
    fig.add_trace(go.Scatter(
        x=resource_data['timestamp'],
        y=resource_data['memory_pct'],
        mode='lines',
        name='Memory Usage (%)'
    ))
    
    fig.add_trace(go.Scatter(
        x=resource_data['timestamp'],
        y=resource_data['io_pct'],
        mode='lines',
        name='I/O Usage (%)'
    ))
    
    fig.update_layout(
        title='Resource Usage Over Time',
        xaxis_title='Time',
        yaxis_title='Usage (%)',
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Add resource usage summary
    col1, col2, col3 = st.columns(3)
    col1.metric("Avg CPU Usage", f"{resource_data['cpu_pct'].mean():.1f}%", 
                f"{resource_data['cpu_pct'].iloc[-1] - resource_data['cpu_pct'].iloc[0]:.1f}%")
    col2.metric("Avg Memory Usage", f"{resource_data['memory_pct'].mean():.1f}%", 
                f"{resource_data['memory_pct'].iloc[-1] - resource_data['memory_pct'].iloc[0]:.1f}%")
    col3.metric("Avg I/O Usage", f"{resource_data['io_pct'].mean():.1f}%", 
                f"{resource_data['io_pct'].iloc[-1] - resource_data['io_pct'].iloc[0]:.1f}%")

def display_execution_history(history_data: Optional[pd.DataFrame] = None):
    """Display pipeline execution history."""
    st.subheader("Pipeline Execution History")
    
    # If no data is provided, generate sample data for demonstration
    if history_data is None:
        # Generate sample data for the last 30 days
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Define some sample pipelines
        pipelines = ['data_ingest', 'transform', 'analytics', 'reporting']
        
        # Generate random data for each pipeline
        data = []
        for date in dates:
            for pipeline in pipelines:
                # Random execution count between 1-5
                executions = np.random.randint(1, 6)
                
                for _ in range(executions):
                    # Random execution duration between 30-120 seconds
                    duration = np.random.randint(30, 121)
                    # Random status with 90% success rate
                    status = 'Success' if np.random.random() < 0.9 else 'Failed'
                    
                    data.append({
                        'date': date,
                        'pipeline': pipeline,
                        'duration_sec': duration,
                        'status': status
                    })
        
        history_data = pd.DataFrame(data)
    
    # Create date filter
    col1, col2 = st.columns(2)
    with col1:
        min_date = history_data['date'].min().date()
        max_date = history_data['date'].max().date()
        start_date = st.date_input("Start Date", min_date)
    with col2:
        end_date = st.date_input("End Date", max_date)
    
    # Filter data based on date range
    filtered_data = history_data[
        (history_data['date'].dt.date >= start_date) & 
        (history_data['date'].dt.date <= end_date)
    ]
    
    # Pipeline selector
    pipelines = sorted(history_data['pipeline'].unique())
    selected_pipelines = st.multiselect("Select Pipelines", pipelines, default=pipelines)
    
    if selected_pipelines:
        filtered_data = filtered_data[filtered_data['pipeline'].isin(selected_pipelines)]
    
    # Show metrics
    total_executions = len(filtered_data)
    success_rate = (filtered_data['status'] == 'Success').mean() * 100
    avg_duration = filtered_data['duration_sec'].mean()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Executions", total_executions)
    col2.metric("Success Rate", f"{success_rate:.1f}%")
    col3.metric("Avg Duration", f"{avg_duration:.1f} sec")
    
    # Create visualizations
    
    # 1. Executions by day and status
    daily_stats = filtered_data.groupby([filtered_data['date'].dt.date, 'status']).size().unstack().fillna(0)
    
    fig = go.Figure()
    
    if 'Success' in daily_stats.columns:
        fig.add_trace(go.Bar(
            x=daily_stats.index,
            y=daily_stats['Success'],
            name='Success',
            marker_color='green'
        ))
    
    if 'Failed' in daily_stats.columns:
        fig.add_trace(go.Bar(
            x=daily_stats.index,
            y=daily_stats['Failed'],
            name='Failed',
            marker_color='red'
        ))
    
    fig.update_layout(
        title='Daily Pipeline Executions',
        xaxis_title='Date',
        yaxis_title='Number of Executions',
        barmode='stack',
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # 2. Average duration by pipeline
    duration_by_pipeline = filtered_data.groupby('pipeline')['duration_sec'].mean().reset_index()
    
    fig = px.bar(
        duration_by_pipeline,
        x='pipeline',
        y='duration_sec',
        color='pipeline',
        labels={'pipeline': 'Pipeline', 'duration_sec': 'Average Duration (s)'},
        title='Average Execution Duration by Pipeline'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # 3. Execution data table
    st.subheader("Execution Details")
    st.dataframe(
        filtered_data.sort_values('date', ascending=False),
        use_container_width=True
    )

def show_metrics():
    """Display the performance metrics page."""
    st.title("Performance Metrics")
    
    # Create tabs for different types of metrics
    tab1, tab2, tab3, tab4 = st.tabs([
        "Overview", 
        "Resource Usage", 
        "Execution History",
        "Pipeline Details"
    ])
    
    with tab1:
        data_loader = DataLoader()
        
        # Option to view the complete dashboard
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
                if f.endswith(".png") and (f.startswith("pipeline_") or f.startswith("stage_"))
            ]
            
            if chart_files:
                for chart_file in chart_files:
                    chart_path = os.path.join(charts_dir, chart_file)
                    st.image(chart_path, caption=chart_file.replace("_", " ").replace(".png", ""))
            else:
                st.info("No performance charts found.")
        else:
            st.info("Performance charts directory not found.")
    
    with tab2:
        # Sample resource usage data
        display_resource_usage()
    
    with tab3:
        # Sample execution history data
        display_execution_history()
    
    with tab4:
        # Get list of pipelines
        data_loader = DataLoader()
        pipeline_names = data_loader.get_available_pipelines()
        
        if pipeline_names:
            selected_pipeline = st.selectbox("Select Pipeline", pipeline_names)
            
            # This would normally come from the API or data loader
            # For now, generate sample data
            pipeline_stages = [
                {"name": "Data Extraction", "avg_duration_ms": 1200, "success_rate": 0.98},
                {"name": "Data Transformation", "avg_duration_ms": 3500, "success_rate": 0.95},
                {"name": "Data Validation", "avg_duration_ms": 800, "success_rate": 0.99},
                {"name": "Data Loading", "avg_duration_ms": 2100, "success_rate": 0.97}
            ]
            
            # Display pipeline stage performance
            st.subheader(f"Pipeline: {selected_pipeline} - Stage Performance")
            
            # Create a DataFrame for the stages
            stages_df = pd.DataFrame(pipeline_stages)
            
            # Plot stage durations
            fig = px.bar(
                stages_df,
                x="name",
                y="avg_duration_ms",
                color="success_rate",
                color_continuous_scale="RdYlGn",
                range_color=[0.9, 1.0],
                labels={"name": "Stage", "avg_duration_ms": "Average Duration (ms)", "success_rate": "Success Rate"},
                title="Pipeline Stage Performance"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Display stage details table
            stages_df["success_rate"] = stages_df["success_rate"].apply(lambda x: f"{x:.2%}")
            st.dataframe(stages_df, use_container_width=True)
        else:
            st.info("No pipelines found.")