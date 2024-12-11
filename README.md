# carrot-transform

TODO: 
* Document carrot-transform 
* Add more comments in-code
* Handle capture of ddl and json config via the command-line as optional args

Reduction in complexity over the original CaRROT-CDM version for the Transform part of *ETL* - In practice *Extract* is always 
performed by Data Partners, *Load* by database bulk-load software.

Sample command line:

`carrot-transform run mapstream \
        --rulesfile=${DATASETHOME}/rules/mapping.json \
        --person-file=${DATASETHOME/input/Demographics.csv \
        --omop-version="5.3" \
        --output-dir=${DATASETHOME}/output \
        ${DATASETHOME}/input`

