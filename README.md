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

## Quick Start

Carrot transform is run from the command line. It now supports poetry to control the python dependencies. To run from the command line, enter:

```
poetry run python carrot_transform.py [args]
```

For example, you can get the version number with:
```
poetry run python carrot_transform.py -v
```

There are many mandatory and optional arguments for carrot transform. In the quick start, we will demonstrate the mandatory arguments on a test case (taken from carrot-CDM) included in the repository. 
Enter the following (as one command):

``` 
poetry run python carrot_transform.py run mapstream carrottransform/examples/test/inputs\
--rules-file\
carrottransform/examples/test/rules/rules_14June2021.json\
--person-file\
carrottransform/examples/test/inputs/Demographics.csv\
--output-dir\
carrottransform/examples/test/test_output\
--omop-ddl-file\
carrottransform/config/OMOPCDM_postgresql_5.3_ddl.sql\
--omop-config-file\
carrottransform/config/omop.json
```

This should create a set of output files in this directory:
```
carrottransform/examples/test/test_output
```



## Arguments
### Required:

```
input-dir,  
	Directory containing input files.	      

--rules-file  
	json file containing mapping rules

--person-file  
	File containing person_ids in the first column  

--output-dir,  
	define the output directory for OMOP-format tsv files  
```


Either:
```
--omop-ddl-file,  
	File containing OHDSI ddl statements for OMOP tables. Instead of specifying the file explicitly, it can be found automatically if --omop-version is specified instead. See --omop-version for further details.
```

AND
```
--omop-config-file,  
    File containing additional/override json config for omop outputs. Instead of specifying the file explicitly, it can be found automatically if --omop-version is specified instead. See --omop-version for further details.
```

OR:
```
--omop-version
	Omop version - e.g., "5.3". Required if neither -omop-ddl-file nor --omop-config-file are set. If this is the case, the software will look for carrottransform/config/omop.json 
	and 
carrottransform/config/OMOPCDM_postgresql_ XX_ddl.sql
to import, where XX is the version number entered as the argument.
```

Optional:
```
--write-mode,  
              default = w  
              options: w, a  
	select whether to write new output files, or append to existing output files  
	  
--saved-person-id-file,  
	Full path to person id file used to save person_id state and share person_ids between data sets
	  
--use-input-person-ids,    
              default = N
              options: Y, N   
	If set to anything other than "N", person ids will be used from the input files. If set to "N" (default behaviour), person ids will be replaced with new integers.
	  
--last-used-ids-file,  
	Full path to last used ids file for OMOP tables. The file should be in a tab separated variable format: 
tablename	last_used_id 
where last_used_id must be an integer.
	  
--log-file-threshold,    
              default = 0
Change the limit for  output count limit for logfile output. Logfile will contain the threshold number of output results.  
```


Reduction in complexity over the original CaRROT-CDM version for the Transform part of _ETL_ - In practice _Extract_ is always
performed by Data Partners, _Load_ by database bulk-load software.

Statistics

External libraries imported (approximate)

carrot-cdm 61
carrot-transform 12
