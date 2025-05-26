
# 🕵️‍♂️ DataTest Pipeline Simulator

**DataTest Pipeline Simulator** is a comprehensive framework for **simulating, testing, and validating data pipelines end-to-end** — with a focus on **PySpark** and **Hadoop-based** workflows. Ensure **data quality**, **schema consistency**, and **performance reliability** across your ETL pipelines, from development to production.

---

## 🚀 Features

### ✅ End-to-End Pipeline Validation
- Test entire ETL workflows, not just isolated components
- Generate comprehensive test reports with success/failure metrics

### 🧪 Pipeline Simulation
- Create and run simulated data pipelines to validate logic and transformations
- Support for common data processing stages: validation, filtering, enrichment, and output

### 📊 Performance Analysis
- Track execution metrics including mean, median, min, and max execution times
- Measure performance at both pipeline and individual stage levels
- Visualize performance data through interactive dashboards

### 📈 Visual Pipeline Reporting
- Generate pipeline flow diagrams using Mermaid.js
- Create interactive HTML dashboards with metrics and execution results
- Export performance data in multiple formats (HTML, Markdown)

### 🧬 Data Quality Checks
- Validate data structure and content throughout the pipeline
- Test data transformations for correctness

---

## 🔧 System Requirements

- Python 3.8 or higher
- PySpark
- Required Python packages (see `requirements.txt`)

---

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/ysrastogi/DataTest-Pipeline-Simulator.git
cd datatest-pipeline-simulator

# Install dependencies
pip install -r requirements.txt

# (Optional) Install in development mode
pip install -e .
```

---

## 🚀 Quick Start

1. **Configure Your Pipeline**  
   Define your pipeline with stages for data validation, transformation, and output.

2. **Run Pipeline Simulation**  
   Execute your pipeline using test data to validate its behavior.

3. **View Results**  
   Check the generated reports in the `outputs/` directory, including:
   - 📉 Pipeline flow diagrams
   - 📊 Performance metrics
   - ✅ Test results
   - 🌐 Interactive dashboards

---

## 🖥️ DataTest CLI Overview

The `datatest` CLI enables powerful interaction with the framework to run pipelines, manage configs, execute tests, and launch API services.

### 🔧 Main Commands

```bash
# Run a pipeline with parameters
datatest run pipeline etl_workflow --params source=s3://bucket/data --params target=hdfs://cluster/output

# Run tests with tag filtering
datatest test --test-dirs tests/unit,tests/integration --tags spark,performance

# Start an interactive shell
datatest shell

# Initialize configuration with a default template
datatest config init --template default

# Start the API server
datatest api --host 0.0.0.0 --port 8082 --reload
```

### 🐚 Interactive Shell Commands

```bash
# Run a specific pipeline
run pipeline <pipeline_name>

# Run performance benchmarks
run benchmark <benchmark_name>

# Run all tests with optional filtering
test --tags performance

# Validate data/config files
validate

# Config management
config init
config list
config get <key>
config show [section]
```

### 💡 CLI Examples

```bash
datatest> run my_pipeline param1=value1 param2=value2
datatest> test --tags performance
datatest> config list
```

---

## 🌐 API Interface

A RESTful API is provided for programmatic and remote control of pipeline operations.

### 🔌 API Endpoints

#### 📁 Pipelines
- `POST /pipelines/run`: Execute a pipeline with parameters

#### 🧪 Tests
- Run tests and retrieve results

#### 📊 Benchmarks
- Run performance benchmarks and retrieve metrics

#### ✅ Validation
- Data and schema validation

#### 📈 Visualization
- Generate reports and dashboards

### 🔐 Authentication

Use **API key authentication**:
- Set `X-API-Key` header in your requests
- Default dev key: `dev-key-for-testing`
- Override via `DATATEST_API_KEY` environment variable

### 🧾 Example API Request

```bash
curl -X POST http://localhost:8082/pipelines/run \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "pipeline_name": "data_transformation",
    "parameters": {
      "input_path": "/data/source",
      "output_path": "/data/target"
    }
  }'
```

---

## 🧪 Getting Started

```bash
# Initialize configuration
datatest config init

# Run tests to ensure setup is correct
datatest test

# Start the API server
datatest api --port 8082
```


---

## 📜 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for more information.


