
> WIP for this https://github.com/Health-Informatics-UoN/carrot-transform/issues/49

[![Carrot Logo](images/logo-primary.png)](https://carrot.ac.uk/transform)

# Development Notes

This document provides notes for contributing to or modifying the Carrot Transform tool.

> If you're just looking to **use** the tool — not develop it — check out the [official documentation](https://carrot.ac.uk/transform).

---

## 🛠 Using `uv`

We use [`uv`](https://docs.astral.sh/uv/) for managing dependencies and running Python scripts.
You don't need to worry about `pip`, `poetry`, `venv`, or even having Python on your PATH — `uv` handles it all.

> Technically, `uv` works with any available Python install — it doesn't require one in your PATH.

### What is `uv`?

[`uv`](https://docs.astral.sh/uv/) is a small command line program that invokes other python programs for you.

`uv` manages tools like `pip`, `poetry`, and `venv` with one fast, modern CLI.
Python's pip is a standard tool, while Poetry and Virtual Environments solved parts of other problems - uv handles all of them with less work from the developer.
It wraps dependency management, virtual environments, and package execution into a single command.

There's also `uvx`, a companion to `uv`, which works like `npx` — letting you run packages without installing them globally.

### Installing `uv`

Follow the [installation guide](https://docs.astral.sh/uv/#installation).
It's surprisingly fast to set up.

---

## 🧩 Dependencies

Want to manage dependencies? Here's how:

- **Add a dependency:**  
  `uv add httpx`

- **Remove a dependency:**  
  `uv remove httpx`

- **Add a dev/test dependency:**  
  `uv add --dev pytest`

See the [official docs](https://docs.astral.sh/uv/concepts/projects/dependencies/) for more details.

### Creating a `.venv/`

If you need a traditional virtual environment for some reason, just run:

```sh
uv sync
```

This creates a `.venv/` you can use like a regular Python virtual environment.

---

## 🧪 Running Tests

We use `pytest` for testing.
You can run all tests with:

```sh
uv run pytest
```

---

## Running from Source

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
> Or just paste it into a text editor and hit `END` `END` `BACKSPACE` `DELETE` until you've reformatted it into one line, then, paste that into the command prompt

> 💡 *Eventually we'd like to auto-detect `--person-file`:*  
> [See GitHub PR #53](https://github.com/Health-Informatics-UoN/carrot-transform/pull/53)

---

## 🚀 Deploying to PyPI

TODO


it looks like the poetry approach ... maybe works?

uv add --dev build

uv pip install build


λ pip install --no-cache-dir build

python -m build
---

## ✅ CI / GitHub Actions

TODO

---
