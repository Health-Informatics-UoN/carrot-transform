# Package entry point - sets up the "run" subcommand
from .subcommands.run import run

import carrottransform as c
import click

@click.group(invoke_without_command=True)
@click.option("--version","-v",is_flag=True)
@click.pass_context
def transform(ctx,version):
    if ctx.invoked_subcommand == None :
        if version:
            click.echo(c.__version__)
        else:
            click.echo(ctx.get_help()) 
        return

transform.add_command(run, "run")

if __name__ == "__main__":
  transform()
