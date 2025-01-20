<p align="center">
  <a href="https://carrot.ac.uk/" target="_blank">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="/images/logo-dark.png">
    <img alt="Carrot Logo" src="/images/logo-primary.png" width="280"/>
  </picture>
  </a>
</p>
<div align="center">
  <strong>
  <h2>Streamlined Data Mapping to OMOP</h2>
  <a href="https://carrot.ac.uk/">Carrot Tranform</a> executes the conversion of the data to the OMOP CDM.<br />
  </strong>
</div>

TODO:

- Document carrot-transform
- Add more comments in-code
- Handle capture of ddl and json config via the command-line as optional args

Reduction in complexity over the original CaRROT-CDM version for the Transform part of _ETL_ - In practice _Extract_ is always
performed by Data Partners, _Load_ by database bulk-load software.

Statistics

External libraries imported (approximate)

carrot-cdm 61
carrot-transform 12
