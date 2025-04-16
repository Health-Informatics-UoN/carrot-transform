
# Development Notes 

This document is meant to be notes for doing development work on this tool.

We're using [uv](https://docs.astral.sh/uv/) so install that - don't worry about anything else.
*Technically* you don't even need python installed.


## pytest

There are pytest tests.
To run them (liek we/I do on Windows)

1. `uv run pytest`

## running from source

1. setup the venv (as for pytest above)
2. install the dependencies
    - `poetry install` [from root](.)
3. make the output dir `mkdir build` or something
3. run the command
    ```
    uv run -m carrottransform.cli.subcommands.run mapstream
            --input-dir carrottransform/examples/test/inputs
            --rules-file  carrottransform/examples/test/rules/rules_14June2021.json
            --person-file carrottransform/examples/test/inputs/Demographics.csv
            --output-dir build
            --omop-ddl-file carrottransform/config/OMOPCDM_postgresql_5.3_ddl.sql
            --omop-config-file carrottransform/config/omop.json
    ```


## building


