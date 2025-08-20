# Package entry point - sets up the "run" subcommand
import click

import carrottransform as c

from .subcommands.run import run
from .subcommands.run_v2 import run_v2


@click.group(invoke_without_command=True)
@click.option("--version", "-v", is_flag=True)
@click.pass_context
def transform(ctx, version):
    if ctx.invoked_subcommand is None:
        if version:
            click.echo(c.__version__)
        else:
            click.echo(ctx.get_help())
        return


transform.add_command(run, "run")
transform.add_command(run_v2, "run_v2")

if __name__ == "__main__":
    transform()
