
# Development Notes 

This document is meant to be notes for doing development work on this tool.

## pytest

There are pytest tests.
To run them (liek we/I do on Windows)

1. setup a [venv](https://docs.python.org/3/library/venv.html) and [poetry](https://python-poetry.org/)
2. install the dependencies
    - `poetry install` [from root](.)
3. run `poetry pytest`
    - ... or `nodemon -x "poetry run pytest" --ext py`
    - at time of writing there's a unit test failure related to the `/` `\` thing on file paths

##
