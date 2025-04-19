"""
Command Line Interface for DataTest Pipeline Simulator.
"""
import click
import os
import sys
from pathlib import Path

# Add project root to path to enable imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.cli.commands import run_cmd, test_cmd, validate_cmd, config_cmd, shell_cmd, api_cmd


@click.group()
@click.version_option(version="0.1.0")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.option("--config-dir", type=click.Path(exists=True), help="Configuration directory")
@click.pass_context
def cli(ctx, verbose, config_dir):
    """DataTest Pipeline Simulator - A tool for testing and benchmarking data pipelines."""
    # Initialize the context object
    ctx.ensure_object(dict)
    ctx.obj["VERBOSE"] = verbose
    ctx.obj["CONFIG_DIR"] = config_dir
    
    # Configure logging based on verbosity
    import logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


# Register commands
cli.add_command(run_cmd)
cli.add_command(test_cmd)
cli.add_command(validate_cmd)
cli.add_command(config_cmd)
cli.add_command(shell_cmd)
cli.add_command(api_cmd)  # Add the new API command


if __name__ == "__main__":
    cli()