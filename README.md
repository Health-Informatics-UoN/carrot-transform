
<p align="center">
  <a href="https://carrot.ac.uk/" target="_blank">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/Health-Informatics-UoN/carrot-transform/refs/heads/main/images/logo-dark.png">
    <img alt="Carrot Logo" src="https://raw.githubusercontent.com/Health-Informatics-UoN/carrot-transform/refs/heads/main/images/logo-primary.png" width="280"/>
  </picture>
  </a>
</p>


<p align="center">

<a href="https://github.com/Health-Informatics-UoN/carrot-transform/releases">
  <img src="https://img.shields.io/github/v/release/Health-Informatics-UoN/carrot-transform" alt="Release">
</a>
<a href="https://opensource.org/license/mit">
  <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License">
</a>
</p>


<div align="center">
  <strong>
  <h2>Streamlined Data Transformation to OMOP</h2><br />
<a href="https://carrot.ac.uk/">Carrot Transform</a> automates data transformation processes and facilitates the standardisation of datasets to the OMOP vocabulary, simplifying the integration of diverse data sources.
  <br />
  </strong>
</div>

<p align="center">
  <br />
  <a href="https://carrot.ac.uk/transform" rel="dofollow"><strong>Explore the docs »</strong></a>
  <br />
<br />  

<a href="https://carrot.ac.uk/">Carrot Mapper</a> is a webapp which allows the user to use the metadata (as output by [WhiteRabbit](https://github.com/OHDSI/WhiteRabbit)) from a dataset to produce mapping rules to the OMOP standard, in the JSON format. These can be ingested by [Carrot Transform](https://carrot.ac.uk/transform/quickstart) to perform the mapping of the contents of the dataset to OMOP.

Carrot Transform transforms input data into tab separated variable files of standard OMOP tables, with  concepts mapped according to the provided rules (generated from Carrot Mapper).

## Quick Start

To have the project up and running, please follow the [Quick Start Guide](https://carrot.ac.uk/transform/quickstart).

If you need to perform development, [there's a brief guide here](https://carrot.ac.uk/transform/development) to get the tool up and running.

## Formatting and Linting

This project is using [ruff](https://docs.astral.sh/ruff/) to check formatting and linting. 
The only dependency is the [`uv` command line tool.](https://docs.astral.sh/uv/)
The `.vscode/tasks.json` file contains a task to run this tool for the currently open file. 
The commands can be run on thier own (in the root folder) like this ...

```bash
# reformat all the files in `./`
λ uv run ruff format .

# run linting checks all the files in `./` 
λ uv run ruff check .

# check and fix all the files in `./`
λ uv run ruff check --fix .

# check and fix all the files in `./` but do so so more eggrsively
λ uv run ruff check --fix --unsafe-fixes .
```

## SQLAlchemy Workflow

Carrot-Transform can read input tables from [SQLAlchemy](https://www.sqlalchemy.org/).
This is experimental, and requires [specifying a connection-string](https://docs.sqlalchemy.org/en/20/tutorial/engine.html) as `--input-db-url` instead of an input dir folder.
The person-file parameter and carrot-mapper workflow should still be used, as if working with .csv files, but carrot-transform can read from an SQLAlchemy database.

1. Extract/export some rows from the various tables
    - something like `SELECT column_name(s) FROM patients LIMIT 1000;` is written to `patients.csv`
2. the usual [scan reports](https://carrot.ac.uk/mapper/user_guide/projects_datasets_scanreports) are performed on these subsets
3. [when carrot-transform is invoked](https://carrot.ac.uk/transform/quickstart) instead of `--input-dir` one specifies `--input-db-url` with a database connection string
    - the `--person-file` parameter should still point to the equivalent of `person_tablename.csv`
    - the `--rules-file` parameter needs to refer to a file on the disk as usual
4. carrot transform will still write data to `--output-dir` and otherwise operate as normal
    - The following parameters have undefined behaviour with this functionality
      - `--write-mode`
      - `--saved-person-id-file`
      - `--use-input-person-ids`
      - `--last-used-ids-file`

## Release Procedure 

To release a new version of `carrot-transform` follow these steps: 

### 1. Prepare the repository
  - First ensure that repository is clean and all required changes have been merged. 
  - Pull the latest changes from `main` with `git pull origin main`. 

### 2. Create a release branch 

- Now create a new feature branch name `release/v<NEW-VERSION>` (e.g. `release/v0.2.0`). 

### 3. Update the version number 
- Use poetry to bump the version. For example, for a minor version update invoke: 
```bash
poetry version minor 
```
- Commit and push the changes (to the release feature branch):
```bash 
NEW_VERSION=$(poetry version -s)
git add pyproject.toml
git commit -m "Bump version to $NEW_VERSION"
git push --set-upstream origin release/v$NEW_VERSION
```

### 4. Create pull request 
- Open a pull request from `release/v$NEW_VERSION` to `main` and await approval.

### 5. Merge and tag 
- After approval merge the the feature branch to `main`. 
- Checkout to `main`, pull updates, and create a tag corresponding to the new version number. 
```bash 
git checkout main
git pull origin main
git tag -a "$NEW_VERSION" -m "Release $NEW_VERSION"
git push origin "$NEW_VERSION"
```

### 6. Create a release

- We must now link the tag to a release in the GitHub repository. To do this from the command line first install GitHub command line tools `gh` and then invoke: 
```bash 
gh release create "$TAG" --title "$TAG" --notes "Release for $VERSION"
```

- Alternatively, follow the instructions in the [GitHub documentation](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository) to manually create a release. 

## License

This repository's source code is available under the [MIT license](LICENSE).
