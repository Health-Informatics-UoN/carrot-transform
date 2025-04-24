
[
![Carrot Logo](
    images/logo-primary.png
)
](https://carrot.ac.uk/transform)

# Development Notes 

This document is meant to be notes for doing development work on this tool.

[
    If you want to "use" the tool without changing how it's programmed - you probably need the website docs linked here.
](https://carrot.ac.uk/transform)

## uv

We're using [uv](https://docs.astral.sh/uv/) to build this.
You don't need to worry about other tool anything else.

> *Technically* you don't need python installed.
> It seems uv will use any available python if it's there.

**`uv`** is a tool designed to replace `poetry`, `pip`, and `venv`.  
It wraps multiple Python packaging and environment management features into a single, fast, and modern CLI — aiming to be the only tool you need for Python development.

### install uv

You can install `uv` by following the instructions [here](https://docs.astral.sh/uv/#installation).  
It's usually shockingly fast to install.

### using uv

Instead of invoking `venv`, `pip`, `poetry`, or even the `python` command directly, you use `uv`.
It simplifies Python workflows into a single, consistent CLI.

> There’s also `uvx`, which works like `npx` — letting you run Python packages on the fly without needing to install them globally.

### dependenhcies

[
    There's a whole manual sectrion for this.
](https://docs.astral.sh/uv/concepts/projects/dependencies/)
These are the things I found myself wondering how to do.

#### add a dependency ...

... like this; `λ uv add httpx`

#### remove a dependency ...

... when it's not needed like this; `λ uv remove httpx`

#### add a dependency to the dev/test stuff ...

... like this `λ uv add --dev pytest`

### getting a `-m venv`

If you need it (for some reason) you can use  `λ uv sync` to setup `.venv/` as a normal python virtual environment.


## pytest

There are pytest tests - you run them like this ...

1. `uv run pytest`

## running from source

You can run the whole 


    uv run -m carrottransform.cli.subcommands.run mapstream \
            --input-dir carrottransform/examples/test/inputs \
            --rules-file  carrottransform/examples/test/rules/rules_14June2021.json \
            --output-dir build \
            --omop-ddl-file carrottransform/config/OMOPCDM_postgresql_5.3_ddl.sql \
            --omop-config-file carrottransform/config/omop.json \







    uv run -m carrottransform.cli.subcommands.run mapstream             --input-dir carrottransform/examples/test/inputs             --rules-file  carrottransform/examples/test/rules/rules_14June2021.json             --output-dir build             --omop-ddl-file carrottransform/config/OMOPCDM_postgresql_5.3_ddl.sql             --omop-config-file carrottransform/config/omop.json 













```

uv run -m carrottransform.cli.subcommands.run mapstream   --input-dir @carrot/examples/test/inputs   --rules-file @carrot/examples/test/rules/rules_14June2021.json   --person-file @carrot/examples/test/inputs/Demographics.csv   --output-dir carrottransform/examples/test/test_output   --omop-ddl-file @carrot/config/OMOPCDM_postgresql_5.3_ddl.sql 
  --omop-config-file @carrot/config/omop.json

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


