"""
Core commands for the DataTest Pipeline Simulator CLI.
"""
import click
import logging
import json
from pathlib import Path
from typing import Dict, List, Any, Optional

from core.config import Configuration

logger = logging.getLogger(__name__)


@click.group(name="run")
@click.pass_context
def run_cmd(ctx):
    """Run pipelines or benchmarks."""
    pass


@run_cmd.command(name="pipeline")
@click.argument("pipeline_name")
@click.option("--params", "-p", multiple=True, help="Pipeline parameters as key=value")
@click.option("--output-dir", "-o", type=click.Path(), help="Output directory")
@click.pass_context
def run_pipeline(ctx, pipeline_name, params, output_dir):
    """Run a specific pipeline."""
    config_dir = ctx.obj.get("CONFIG_DIR")
    config = Configuration(Path(config_dir) if config_dir else None)
    
    # Parse parameters
    parameters = {}
    for param in params:
        key, value = param.split("=", 1)
        parameters[key] = value
    
    click.echo(f"Running pipeline: {pipeline_name}")
    click.echo(f"Parameters: {parameters}")
    
    # TODO: Implement actual pipeline execution
    # from src.pipeline.runner import PipelineRunner
    # runner = PipelineRunner()
    # result = runner.run_pipeline(pipeline_name, parameters)


@run_cmd.command(name="benchmark")
@click.argument("benchmark_name")
@click.option("--iterations", "-i", type=int, default=1, help="Number of iterations")
@click.option("--parallel", "-p", type=int, default=1, help="Number of parallel runs")
@click.option("--warmup", "-w", type=int, default=0, help="Number of warm-up iterations")
@click.option("--output-dir", "-o", type=click.Path(), default="benchmark_results", help="Output directory")
@click.pass_context
def run_benchmark(ctx, benchmark_name, iterations, parallel, warmup, output_dir):
    """Run a benchmark for performance testing."""
    config_dir = ctx.obj.get("CONFIG_DIR")
    config = Configuration(Path(config_dir) if config_dir else None)
    
    click.echo(f"Running benchmark: {benchmark_name}")
    click.echo(f"Iterations: {iterations}, Parallel: {parallel}, Warmup: {warmup}")
    
    # TODO: Implement actual benchmark execution
    # from src.benchmarking.runner import BenchmarkRunner
    # runner = BenchmarkRunner(output_dir=output_dir)
    # result = runner.run_benchmark(benchmark_name, iterations, parallel, warmup)


@click.command(name="test")
@click.option("--test-dirs", "-d", multiple=True, default=["tests"], help="Test directories")
@click.option("--pattern", "-p", default="*_test.py", help="Test file pattern")
@click.option("--tags", "-t", multiple=True, help="Filter tests by tags")
@click.option("--output-dir", "-o", default="test_reports", help="Output directory for test reports")
@click.pass_context
def test_cmd(ctx, test_dirs, pattern, tags, output_dir):
    """Run tests for the pipeline simulator."""
    from src.testing.runner import TestRunner
    
    click.echo(f"Running tests from directories: {', '.join(test_dirs)}")
    if tags:
        click.echo(f"Filtering by tags: {', '.join(tags)}")
    
    runner = TestRunner(output_dir=output_dir)
    result = runner.discover_and_run(
        test_dirs=list(test_dirs), 
        pattern=pattern, 
        tags=list(tags) if tags else None
    )
    
    # Fix: Use the correct keys from the result dictionary
    total = result.get('total', 0)
    passed = result.get('passed', 0)
    failed = result.get('failed', 0)
    error = result.get('error', 0)
    skipped = result.get('skipped', 0)
    
    click.echo(f"Tests completed: {total} tests run")
    click.echo(f"Passed: {passed}, Failed: {failed}, Error: {error}, Skipped: {skipped}")
    
    if failed > 0 or error > 0:
        ctx.exit(1)


@click.command(name="validate")
@click.argument("target")
@click.option("--schema", "-s", help="Schema file to validate against")
@click.option("--config", "-c", help="Configuration file")
@click.pass_context
def validate_cmd(ctx, target, schema, config):
    """Validate data or configuration files."""
    click.echo(f"Validating: {target}")
    if schema:
        click.echo(f"Using schema: {schema}")
    
    # TODO: Implement validation logic
    # from src.validation.validator import Validator
    # validator = Validator()
    # result = validator.validate(target, schema)
    
    click.echo("Validation complete")


@click.command(name="api")
@click.option("--host", default="0.0.0.0", help="Host to bind the API server")
@click.option("--port", default=8082, type=int, help="Port to bind the API server")
@click.option("--reload", is_flag=True, help="Enable auto-reload for development")
@click.pass_context
def api_cmd(ctx, host, port, reload):
    """Start the API server."""
    try:
        import uvicorn
        from src.api.main import app
    except ImportError:
        click.echo("Error: uvicorn and fastapi packages are required to run the API server.")
        click.echo("Install them with: pip install uvicorn fastapi")
        ctx.exit(1)
    
    click.echo(f"Starting API server at http://{host}:{port}")
    uvicorn.run("src.api.main:app", host=host, port=port, reload=reload)


@click.group(name="config")
@click.pass_context
def config_cmd(ctx):
    """Manage configuration for the pipeline simulator."""
    pass


@config_cmd.command(name="init")
@click.option("--force", "-f", is_flag=True, help="Force initialization even if config exists")
@click.pass_context
def config_init(ctx, force):
    """Initialize configuration files."""
    config_dir = ctx.obj.get("CONFIG_DIR")
    click.echo(f"Initializing configuration in: {config_dir or 'default location'}")
    
    # TODO: Implement actual config initialization
    # from src.config.initializer import ConfigInitializer
    # initializer = ConfigInitializer()
    # initializer.init(force=force)
    
    click.echo("Configuration initialized successfully")


@config_cmd.command(name="show")
@click.argument("section", required=False)
@click.pass_context
def config_show(ctx, section):
    """Show current configuration."""
    config_dir = ctx.obj.get("CONFIG_DIR")
    from core.config import Configuration
    
    config = Configuration(Path(config_dir) if config_dir else None)
    
    if section:
        click.echo(f"Configuration section: {section}")
        # Show specific section
        # section_config = config.get_section(section)
        # click.echo(json.dumps(section_config, indent=2))
    else:
        click.echo("Full configuration:")
        # Show full config
        # click.echo(json.dumps(config.get_all(), indent=2))
    
    click.echo("Configuration displayed successfully")


@click.command(name="shell")
@click.pass_context
def shell_cmd(ctx):
    """Launch an interactive shell with the simulator environment."""
    try:
        import IPython
        from traitlets.config import Config
    except ImportError:
        click.echo("Error: IPython is required for the shell command.")
        click.echo("Install it with: pip install ipython")
        ctx.exit(1)
    
    click.echo("Starting DataTest Pipeline Simulator interactive shell...")
    
    # Create IPython configuration
    ipython_config = Config()
    ipython_config.TerminalInteractiveShell.banner1 = """
    DataTest Pipeline Simulator Interactive Shell
    Version: 0.1.0
    
    Available modules:
    - core.config: Configuration management
    - src.pipeline: Pipeline execution tools
    - src.testing: Testing utilities
    
    Type '?' after an object to get help.
    """
    
    # Setup shell context
    config_dir = ctx.obj.get("CONFIG_DIR")
    from core.config import Configuration
    
    shell_context = {
        "config": Configuration(Path(config_dir) if config_dir else None),
        "Path": Path,
        # Add other useful objects to the context
    }
    
    # Start IPython shell
    IPython.start_ipython(argv=[], config=ipython_config, user_ns=shell_context)