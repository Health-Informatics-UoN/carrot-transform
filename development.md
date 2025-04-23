
# Development Notes 

This document is meant to be notes for doing development work on this tool.

We're using [uv](https://docs.astral.sh/uv/) so install that - don't worry about anything else.
*Technically* you don't need python installed, but, it looks like uv will use it if it's there.

## uv

**`uv`** is a tool designed to replace `poetry`, `pip`, and `venv`.  
It wraps multiple Python packaging and environment management features into a single, fast, and modern CLI — aiming to be the only tool you need for Python development.

### install uv

You can install `uv` by following the instructions [here](https://docs.astral.sh/uv/#installation).  
It’s usually shockingly fast to install.

### using uv

Instead of invoking `venv`, `pip`, `poetry`, or even the `python` command directly, you can just use `uv`. It simplifies Python workflows into a single, consistent CLI.
There’s also `uvx`, which works like `npx` — letting you run Python packages on the fly without needing to install them globally.

## pytest

There are pytest tests.
To run them (liek we/I do on Windows)

1. `uv run pytest`

## running from source

> this needs to be update for uv

1. setup the venv (as for pytest above)
2. install the dependencies
    - `poetry install` [from root](.)
3. make the output dir `mkdir build` or something
3. run the command
    ```
    uv run -m carrottransform.cli.subcommands.run mapstream \
            --input-dir carrottransform/examples/test/inputs \
            --rules-file  carrottransform/examples/test/rules/rules_14June2021.json \
            --output-dir build \
            --omop-ddl-file carrottransform/config/OMOPCDM_postgresql_5.3_ddl.sql \
            --omop-config-file carrottransform/config/omop.json \
    ```

## building


