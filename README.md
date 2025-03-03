<p align="center">
  <a href="https://carrot.ac.uk/" target="_blank">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="/images/logo-dark.png">
    <img alt="Carrot Logo" src="/images/logo-primary.png" width="280"/>
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

## Quick Start for Developers

To have the project up and running, please follow the [Quick Start Guide](https://carrot.ac.uk/transform/quickstart).

## Release Procedure 
To release a new version of `carrot-transform` first ensure that repository is clean and all required changes have been merged. 
Now create a new pull request on a new feature branch and update the `pyproject.toml` to the new required semantic version. 
You can use poetry to do this automatically. 
For example, for a minor version update invoke: 
```bash
poetry version minor 
```
Commit and push the changes (to the release feature branch):
```bash 
git add pyproject.toml
git commit -m "Bump version to <NEW-VERSION>"
git push 
```
After approval merge the the feature branch to main and create a tag corresponding to the new version number. For example, if the new version number is `0.2.0`
```bash 
git tag -a "0.2.0" -m "Release 0.2.0"
git push origin "0.2.0"
```
We must now link the tag to a release in the GitHub repository. To do this from the command line first install GitHub command line tools `gh` and then invoke: 
```bash 
gh release create "$TAG" --title "$TAG" --notes "Automated release for $VERSION"
```

Alternatively, follow the instructions [on the GitHub website](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository) to manually create a  release. 
## License

This repository's source code is available under the [MIT license](LICENSE).

