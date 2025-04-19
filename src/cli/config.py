"""
Configuration management commands for the DataTest Pipeline Simulator CLI.
"""
import click
import os
import json
import yaml
from pathlib import Path

from core.config import Configuration


@click.group(name="config")
def config_cmd():
    """Manage configuration settings."""
    pass


@config_cmd.command(name="get")
@click.argument("key")
@click.option("--format", "-f", type=click.Choice(["json", "yaml", "text"]), default="text", help="Output format")
@click.pass_context
def get_config(ctx, key, format):
    """Get a configuration value."""
    config_dir = ctx.obj.get("CONFIG_DIR")
    config = Configuration(Path(config_dir) if config_dir else None)
    
    value = config.get(key)
    if value is None:
        click.echo(f"Configuration key '{key}' not found", err=True)
        ctx.exit(1)
    
    if format == "json":
        click.echo(json.dumps(value, indent=2))
    elif format == "yaml":
        click.echo(yaml.dump(value, default_flow_style=False))
    else:
        click.echo(value)


@config_cmd.command(name="list")
@click.option("--format", "-f", type=click.Choice(["json", "yaml", "text"]), default="text", help="Output format")
@click.pass_context
def list_config(ctx, format):
    """List all configuration settings."""
    config_dir = ctx.obj.get("CONFIG_DIR")
    config = Configuration(Path(config_dir) if config_dir else None)
    
    settings = config.settings
    
    if format == "json":
        click.echo(json.dumps(settings, indent=2))
    elif format == "yaml":
        click.echo(yaml.dump(settings, default_flow_style=False))
    else:
        for key, value in sorted(settings.items()):
            click.echo(f"{key}: {value}")


@config_cmd.command(name="init")
@click.option("--template", "-t", type=click.Choice(["default", "minimal"]), default="default", help="Template to use")
@click.pass_context
def init_config(ctx, template):
    """Initialize a new configuration."""
    from config.settings import CONFIG_DIR
    
    if not os.path.exists(CONFIG_DIR):
        os.makedirs(CONFIG_DIR)
        click.echo(f"Created configuration directory: {CONFIG_DIR}")
    
    # TODO: Implement configuration template copying
    click.echo(f"Initialized configuration using {template} template")