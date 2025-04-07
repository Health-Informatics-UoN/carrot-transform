
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

## running from source

1. setup the venv (as for pytest above)
2. install the dependencies
    - `poetry install` [from root](.)
3. make the output dir `mkdir build` or something
3. run the command
    ```
    carrot-transform run mapstream
        carrottransform/examples/test/inputs
        --rules-file  carrottransform/examples/test/rules/rules_14June2021.json
        --person-file carrottransform/examples/test/inputs/Demographics.csv
        --output-dir build
        --omop-ddl-file carrottransform/config/OMOPCDM_postgresql_5.3_ddl.sql
        --omop-config-file carrottransform/config/omop.json
    ```