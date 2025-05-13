

This document provides notes for contributing to or modifying the Carrot Transform tool.

> If you only need to **use** carrot-transform - not develop it - check out the [official documentation](https://carrot.ac.uk/transform).

---

# Development Notes

## Using `uv`

We use [`uv`](https://docs.astral.sh/uv/) for managing dependencies and running Python scripts.

### What is `uv`?

[`uv`](https://docs.astral.sh/uv/) is a small command line program that invokes other python tools.
There's also `uvx`, a companion to `uv`, which works like `npx` or `pipx` to run packages without installing them to a project.

### Installing `uv`

Follow the [installation guide](https://docs.astral.sh/uv/#installation).
It's surprisingly fast to set up.

---

### Running Tests

We use `pytest` for testing.
You can run all tests with:

```sh
uv run pytest
```

---

### Running from Source

You can run the CLI directly like this:

```sh
uv run -m carrottransform.cli.subcommands.run mapstream \
    --input-dir carrottransform/examples/test/inputs \
    --person-file carrottransform/examples/test/inputs/Demographics.csv \
    --rules-file carrottransform/examples/test/rules/rules_14June2021.json \
    --output-dir build \
    --omop-ddl-file carrottransform/config/OMOPCDM_postgresql_5.3_ddl.sql \
    --omop-config-file carrottransform/config/omop.json
```

> ⚠️ On Windows, you would need to replace `/` with `^` for line continuation in the terminal.
>
> Or just paste it into a text editor and hit `END` `END` `BACKSPACE` `DELETE` until you've reformatted it into one line, then, paste that into the command prompt

> 💡 *Eventually we'd like to auto-detect `--person-file`:*  
> [See GitHub PR #53](https://github.com/Health-Informatics-UoN/carrot-transform/pull/53)

---

## Deploying to PyPI and CI / GitHub Actions

[The CI file](.github/workflows/uv-workflow.yml) uses [GitHub actions](https://github.com/Health-Informatics-UoN/carrot-transform/actions) to test the project and (when appropriate) [deploy to PyPI.](https://pypi.org/project/carrot-transform/)
When the CI job runs, it tests and assembles the project.
If the commit being examined is against the `main` branch, then, the project will be deployed to PyPI Test.
If the commit has been tagged with a tag starting with `v...` then the project will be deployed to PyPI Main.
