import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Optional
import json
import os
from dashboard.utils.data_loader import DataLoader

def show_data_quality():
    """Display the data quality visualization page."""
    st.title("Data Quality Visualizations")
    
    # Create tabs for different data quality aspects
    tab1, tab2, tab3, tab4 = st.tabs([
        "Overview", 
        "Data Profiles", 
        "Validation Results",
        "Anomaly Detection"
    ])
    
    with tab1:
        # Data quality overview
        st.header("Data Quality Overview")
        
        # Sample pipelines
        pipelines = ["sales_data", "customer_data", "inventory_data", "marketing_data"]
        
        # Generate sample data quality scores
        data_quality_scores = {
            "completeness": [0.95, 0.92, 0.98, 0.94],
            "accuracy": [0.92, 0.88, 0.94, 0.91],
            "consistency": [0.94, 0.95, 0.96, 0.93],
            "timeliness": [0.98, 0.92, 0.91, 0.97],
            "uniqueness": [0.96, 0.99, 0.97, 0.95]
        }
        
        # Create a DataFrame for the quality scores
        quality_df = pd.DataFrame(data_quality_scores, index=pipelines)
        
        # Display metrics
        avg_quality = quality_df.mean().mean()
        min_quality = quality_df.min().min()
        min_quality_dimension = quality_df.min().idxmin()
        min_quality_pipeline = quality_df.min(axis=1).idxmin()
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Overall Data Quality", f"{avg_quality:.1%}")
        col2.metric("Lowest Quality Score", f"{min_quality:.1%}")
        col3.metric("Area to Improve", f"{min_quality_dimension} in {min_quality_pipeline}")
        
        # Create radar chart for data quality dimensions
        fig = go.Figure()
        
        dimensions = list(data_quality_scores.keys())
        
        for pipeline in pipelines:
            fig.add_trace(go.Scatterpolar(
                r=quality_df.loc[pipeline].values,
                theta=dimensions,
                fill='toself',
                name=pipeline
            ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0.8, 1]
                )),
            showlegend=True,
            title="Data Quality Dimensions by Pipeline"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Display data quality metrics table
        st.subheader("Data Quality Metrics")
        
        # Format percentages in the DataFrame
        formatted_df = quality_df.applymap(lambda x: f"{x:.1%}")
        st.dataframe(formatted_df, use_container_width=True)
    
    with tab2:
        # Data profiling
        st.header("Data Profiles")
        
        # Pipeline selector for profiling
        pipelines = ["sales_data", "customer_data", "inventory_data", "marketing_data"]
        selected_pipeline = st.selectbox("Select Pipeline for Profiling", pipelines)
        
        # Generate sample profile data
        if selected_pipeline == "sales_data":
            profile_data = {
                "columns": [
                    {"name": "order_id", "type": "string", "unique": 100.0, "missing": 0.0, "stats": {"min": None, "max": None}},
                    {"name": "customer_id", "type": "string", "unique": 78.5, "missing": 0.1, "stats": {"min": None, "max": None}},
                    {"name": "product_id", "type": "string", "unique": 45.2, "missing": 0.0, "stats": {"min": None, "max": None}},
                    {"name": "order_date", "type": "datetime", "unique": 98.7, "missing": 0.0, "stats": {"min": "2023-01-01", "max": "2023-12-31"}},
                    {"name": "quantity", "type": "integer", "unique": 15.3, "missing": 0.0, "stats": {"min": 1, "max": 100}},
                    {"name": "price", "type": "float", "unique": 55.7, "missing": 0.0, "stats": {"min": 5.99, "max": 999.99}},
                    {"name": "discount", "type": "float", "unique": 12.8, "missing": 8.2, "stats": {"min": 0.0, "max": 0.5}}
                ]
            }
        else:
            # Generate generic profile data for other pipelines
            column_names = {
                "customer_data": ["customer_id", "name", "email", "phone", "address", "registration_date", "loyalty_score"],
                "inventory_data": ["product_id", "name", "category", "stock_quantity", "reorder_level", "supplier_id", "last_restock_date"],
                "marketing_data": ["campaign_id", "name", "start_date", "end_date", "budget", "impressions", "clicks", "conversions"]
            }
            
            columns = []
            for col in column_names.get(selected_pipeline, ["col1", "col2", "col3"]):
                # Generate random profile stats
                unique_pct = np.random.uniform(50, 100) if "id" in col else np.random.uniform(10, 90)
                missing_pct = np.random.uniform(0, 15) if "id" not in col else 0
                
                col_type = "string"
                stats = {"min": None, "max": None}
                
                if "date" in col:
                    col_type = "datetime"
                    stats = {"min": "2023-01-01", "max": "2023-12-31"}
                elif any(x in col for x in ["quantity", "level", "score"]):
                    col_type = "integer"
                    stats = {"min": np.random.randint(0, 10), "max": np.random.randint(50, 100)}
                elif any(x in col for x in ["price", "budget", "cost"]):
                    col_type = "float"
                    stats = {"min": round(np.random.uniform(0, 100), 2), "max": round(np.random.uniform(500, 10000), 2)}
                
                columns.append({
                    "name": col,
                    "type": col_type,
                    "unique": unique_pct,
                    "missing": missing_pct,
                    "stats": stats
                })
            
            profile_data = {"columns": columns}
        
        # Display column profiles
        st.subheader(f"Column Profiles for {selected_pipeline}")
        
        # Create DataFrame for column profiles
        profile_df = pd.DataFrame([
            {
                "Column": col["name"],
                "Type": col["type"],
                "Unique %": f"{col['unique']:.1f}%",
                "Missing %": f"{col['missing']:.1f}%",
                "Min": col["stats"]["min"],
                "Max": col["stats"]["max"]
            }
            for col in profile_data["columns"]
        ])
        
        st.dataframe(profile_df, use_container_width=True)
        
        # Create visualizations for missing values and uniqueness
        missing_data = [{"column": col["name"], "missing": col["missing"]} for col in profile_data["columns"]]
        missing_df = pd.DataFrame(missing_data)
        
        if not missing_df.empty and missing_df["missing"].sum() > 0:
            st.subheader("Missing Values by Column")
            fig = px.bar(
                missing_df,
                x="column",
                y="missing",
                color="missing",
                labels={"column": "Column", "missing": "Missing Values (%)"},
                color_continuous_scale="Reds",
                title="Missing Values by Column"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Create uniqueness visualization
        unique_data = [{"column": col["name"], "unique": col["unique"]} for col in profile_data["columns"]]
        unique_df = pd.DataFrame(unique_data)
        
        st.subheader("Column Uniqueness")
        fig = px.bar(
            unique_df,
            x="column",
            y="unique",
            color="unique",
            labels={"column": "Column", "unique": "Unique Values (%)"},
            color_continuous_scale="Blues",
            title="Uniqueness by Column"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        # Data validation results
        st.header("Validation Results")
        
        # Pipeline selector for validation results
        pipelines = ["sales_data", "customer_data", "inventory_data", "marketing_data"]
        selected_pipeline = st.selectbox("Select Pipeline", pipelines, key="validation_pipeline")
        
        # Generate sample validation results
        validation_rules = [
            {"name": "Non-null Check", "type": "not_null", "column": "order_id", "passed": True, "records_failed": 0},
            {"name": "Unique Check", "type": "unique", "column": "order_id", "passed": True, "records_failed": 0},
            {"name": "Range Check", "type": "range", "column": "quantity", "passed": False, "records_failed": 15, "details": "Values must be between 1 and 100"},
            {"name": "Format Check", "type": "format", "column": "email", "passed": False, "records_failed": 8, "details": "Must be a valid email format"},
            {"name": "Relationship Check", "type": "relationship", "column": "customer_id", "passed": True, "records_failed": 0},
            {"name": "Freshness Check", "type": "freshness", "column": "order_date", "passed": True, "records_failed": 0},
            {"name": "Custom Business Rule", "type": "custom", "column": "discount", "passed": False, "records_failed": 12, "details": "Discount cannot exceed 50%"}
        ]
        
        # Calculate validation metrics
        total_rules = len(validation_rules)
        passed_rules = sum(1 for rule in validation_rules if rule["passed"])
        failed_rules = total_rules - passed_rules
        pass_rate = passed_rules / total_rules if total_rules > 0 else 0
        total_failures = sum(rule["records_failed"] for rule in validation_rules)
        
        # Display validation metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Rules Passed", f"{passed_rules}/{total_rules}", f"{pass_rate:.1%}")
        col2.metric("Failed Rules", failed_rules, "")
        col3.metric("Records with Issues", total_failures, "")
        
        # Create visualization for validation results
        st.subheader("Validation Rule Results")
        
        validation_df = pd.DataFrame(validation_rules)
        
        # Add a status column for coloring
        validation_df["status"] = validation_df["passed"].apply(lambda x: "Passed" if x else "Failed")
        
        fig = px.bar(
            validation_df,
            x="name",
            y="records_failed",
            color="status",
            color_discrete_map={"Passed": "green", "Failed": "red"},
            labels={"name": "Validation Rule", "records_failed": "Failed Records"},
            title="Validation Results by Rule"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Display validation details table
        st.subheader("Validation Rule Details")
        
        # Create a display DataFrame with formatted columns
        display_df = validation_df[["name", "type", "column", "passed", "records_failed", "details"]].copy()
        display_df["passed"] = display_df["passed"].apply(lambda x: "✅" if x else "❌")
        
        st.dataframe(display_df, use_container_width=True)
    
    with tab4:
        # Anomaly detection
        st.header("Anomaly Detection")
        
        # Pipeline and metric selectors
        pipelines = ["sales_data", "customer_data", "inventory_data", "marketing_data"]
        selected_pipeline = st.selectbox("Select Pipeline", pipelines, key="anomaly_pipeline")
        
        metrics = ["Record Count", "Average Value", "Null Rate", "Error Rate"]
        selected_metric = st.selectbox("Select Metric", metrics)
        
        # Generate sample anomaly data (time series with anomalies)
        # For demonstration, we'll create 30 days of data with a few anomalies
        np.random.seed(42)  # For reproducibility
        
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        
        # Create base metric with weekly seasonality
        base_value = 100
        if selected_metric == "Record Count":
            base_value = 1000
        elif selected_metric == "Average Value":
            base_value = 50
        elif selected_metric == "Null Rate":
            base_value = 5
        elif selected_metric == "Error Rate":
            base_value = 2
        
        # Add weekly seasonality
        seasonality = np.sin(np.arange(len(dates)) * (2 * np.pi / 7)) * (base_value * 0.2)
        
        # Add trend
        trend = np.linspace(0, base_value * 0.1, len(dates))
        
        # Add noise
        noise = np.random.normal(0, base_value * 0.05, len(dates))
        
        # Combine components
        values = base_value + seasonality + trend + noise
        
        # Add anomalies
        anomaly_indices = [7, 15, 22]
        for idx in anomaly_indices:
            if idx < len(values):
                # Make the anomaly an outlier (3-5x the standard deviation)
                values[idx] = values[idx] + np.random.choice([-1, 1]) * np.random.uniform(3, 5) * np.std(values)
        
        # Create a DataFrame with the time series and anomaly indicators
        anomaly_data = pd.DataFrame({
            "date": dates,
            "value": values,
            "is_anomaly": [1 if i in anomaly_indices else 0 for i in range(len(dates))]
        })
        
        # Display the anomaly time series
        st.subheader(f"Anomaly Detection: {selected_metric} for {selected_pipeline}")
        
        fig = go.Figure()
        
        # Add the main time series
        fig.add_trace(go.Scatter(
            x=anomaly_data["date"],
            y=anomaly_data["value"],
            mode="lines+markers",
            name=selected_metric,
            line=dict(color="blue")
        ))
        
        # Highlight anomalies
        anomalies = anomaly_data[anomaly_data["is_anomaly"] == 1]
        fig.add_trace(go.Scatter(
            x=anomalies["date"],
            y=anomalies["value"],
            mode="markers",
            name="Anomalies",
            marker=dict(color="red", size=12, symbol="circle-open")
        ))
        
        fig.update_layout(
            title=f"{selected_metric} Over Time with Detected Anomalies",
            xaxis_title="Date",
            yaxis_title=selected_metric,
            hovermode="x unified"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Display anomaly details
        if not anomalies.empty:
            st.subheader("Detected Anomalies")
            
            # Calculate the expected range
            rolling_mean = anomaly_data["value"].rolling(window=5, center=True).mean()
            rolling_std = anomaly_data["value"].rolling(window=5, center=True).std()
            
            # Create a display table for anomalies
            anomaly_details = []
            for _, row in anomalies.iterrows():
                idx = anomaly_data[anomaly_data["date"] == row["date"]].index[0]
                
                # Get the expected range at this point
                expected_mean = rolling_mean.iloc[idx] if idx < len(rolling_mean) else np.nan
                expected_std = rolling_std.iloc[idx] if idx < len(rolling_std) else np.nan
                
                if np.isnan(expected_mean) or np.isnan(expected_std):
                    deviation = "N/A"
                    expected_range = "N/A"
                else:
                    deviation = (row["value"] - expected_mean) / expected_std
                    lower_bound = expected_mean - 2 * expected_std
                    upper_bound = expected_mean + 2 * expected_std
                    expected_range = f"{lower_bound:.2f} - {upper_bound:.2f}"
                
                anomaly_details.append({
                    "Date": row["date"].strftime("%Y-%m-%d"),
                    "Value": f"{row['value']:.2f}",
                    "Expected Range": expected_range,
                    "Deviation (σ)": f"{deviation:.2f}σ" if deviation != "N/A" else deviation,
                    "Severity": "High" if abs(deviation) > 4 else "Medium" if abs(deviation) > 3 else "Low"
                })
            
            anomaly_df = pd.DataFrame(anomaly_details)
            st.dataframe(anomaly_df, use_container_width=True)
        else:
            st.info("No anomalies detected in the selected data.")