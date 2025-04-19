"""
Interactive shell for the DataTest Pipeline Simulator CLI.
"""
import click
import os
import sys
import cmd
from pathlib import Path

from core.config import Configuration


class DataTestShell(cmd.Cmd):
    """Interactive shell for DataTest Pipeline Simulator."""
    
    intro = "Welcome to the DataTest Pipeline Simulator shell. Type help or ? to list commands.\n"
    prompt = "datatest> "
    
    def __init__(self, ctx):
        super().__init__()
        self.ctx = ctx
        config_dir = ctx.obj.get("CONFIG_DIR")
        self.config = Configuration(Path(config_dir) if config_dir else None)
    
    def do_run(self, arg):
        """Run a pipeline: run <pipeline_name> [param1=value1 param2=value2 ...]"""
        args = arg.split()
        if not args:
            print("Pipeline name is required")
            return
        
        pipeline_name = args[0]
        params = {}
        for param in args[1:]:
            if "=" in param:
                key, value = param.split("=", 1)
                params[key] = value
        
        print(f"Running pipeline: {pipeline_name}")
        print(f"Parameters: {params}")
        # TODO: Implement actual pipeline execution
    
    def do_test(self, arg):
        """Run tests: test [--tags tag1,tag2] [--pattern pattern]"""
        from src.testing.runner import TestRunner
        
        args = arg.split()
        tags = []
        pattern = "*_test.py"
        test_dirs = ["tests"]
        
        i = 0
        while i < len(args):
            if args[i] == "--tags" and i + 1 < len(args):
                tags = args[i + 1].split(",")
                i += 2
            elif args[i] == "--pattern" and i + 1 < len(args):
                pattern = args[i + 1]
                i += 2
            elif args[i] == "--test-dirs" and i + 1 < len(args):
                test_dirs = args[i + 1].split(",")
                i += 2
            else:
                i += 1
        
        print(f"Running tests from directories: {', '.join(test_dirs)}")
        if tags:
            print(f"Filtering by tags: {', '.join(tags)}")
        
        runner = TestRunner()
        result = runner.discover_and_run(
            test_dirs=test_dirs,
            pattern=pattern,
            tags=tags if tags else None
        )
        
        print(f"Tests completed: {result['total_tests']} tests run")
        print(f"Passed: {result['passed']}, Failed: {result['failed']}, Skipped: {result['skipped']}")
    
    def do_config(self, arg):
        """Get or list configuration: config get <key> | config list"""
        args = arg.split()
        if not args:
            print("Subcommand required: get or list")
            return
        
        if args[0] == "get" and len(args) > 1:
            key = args[1]
            value = self.config.get(key)
            if value is None:
                print(f"Configuration key '{key}' not found")
            else:
                print(f"{key}: {value}")
        elif args[0] == "list":
            for key, value in sorted(self.config.settings.items()):
                print(f"{key}: {value}")
        else:
            print("Unknown subcommand. Use 'config get <key>' or 'config list'")
    
    def do_exit(self, arg):
        """Exit the shell"""
        print("Goodbye!")
        return True
    
    def do_quit(self, arg):
        """Exit the shell"""
        return self.do_exit(arg)


@click.command(name="shell")
@click.pass_context
def shell_cmd(ctx):
    """Start an interactive shell session."""
    shell = DataTestShell(ctx)
    shell.cmdloop()