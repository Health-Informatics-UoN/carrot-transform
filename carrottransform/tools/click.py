import click
from pathlib import Path


def PathArgs():
    """used by the click library for CLI args that are files"""

    class PathArgs(click.ParamType):
        name = "pathlib.Path"

        def convert(self, value, param, ctx):
            try:
                return Path(value)
            except Exception as e:
                self.fail(f"Invalid path: {value} ({e})", param, ctx)

    return PathArgs()


# use this
PathArgs = PathArgs()
