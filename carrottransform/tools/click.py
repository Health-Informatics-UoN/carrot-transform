import click
from pathlib import Path


from sqlalchemy import create_engine
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


def AlchemyEngine():

    """should enforce an sql alchemy thing"""

    class AlchemyEngine(click.ParamType):
        name = "sqlalchemy.engine.Engine"

        def convert(self, value, param, ctx):
            try:
                return create_engine(value)
            except Exception as e:
                self.fail(f"Invalid sqlalchemy connection string: {value} ({e})", param, ctx)

    return AlchemyEngine()

AlchemyEngine = AlchemyEngine()