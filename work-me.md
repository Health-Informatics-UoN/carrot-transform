
delete this before merging with main

this branch has the functionality to read/write from s3 buckets as if they're `.tsv` or `.csv`
reading is pretty inefficent (sorry) and really is just used for testing - writing is done via a streaming approach.
oddly; these are pretty simmilar, so, reading could be improved - likely faster than i was able to inegrate v2.

it does that by reworkign the input and output to function behind an API instead of the intersections of options they used prior

to switch between the two, we/i change the "output-dir" arg to now specify either a path to where the output should be, or, an `s3:<bucket name>` value.
from there the arg type (thing) instantiates something to write the tables to.


- done
    - [x] read/verify the results of the test
        - working with a jig
        - "the couch" works
        - [x] need a filename or something to actually read data from
            - login to s3 with phone
            - found "observation"
        - [x] probably going to add expected outputs to the test_data/ as .tsv files
        - [x] gettir for the actual test
            - something weird with the closing is happening here too. i suspect another `with`
        - cmd: `λ uv run pytest tests/test_outputs.py::test_s3read --log-cli-level=INFO -m s3tests`
        - cmd: `λ uv run pytest tests/test_outputs.py::test_s3run --log-cli-level=INFO -m s3tests`
- open
    - [ ] update old tests to pass with the new parameter structure
        - [x] tests/test_run.py::test_nonexistent_file
        - [x] tests/test_run.py::test_directory_not_found
        - [x] tests/test_integration.py::test_sql_read
        - [x] tests/test_integration.py::test_fixture
        - [x] add the env-arg or cli-param tests
    - [x] cleanup the logic between old and new tests
        - it's becoming a case of rewrite the tests
        - ... but ... they're verifying the whole results so that's probably better
- bug the source.SourceObject needs to block trailing empty columsn
- for v1
    - [x] change `output-dir` to just `output`
    - [x] remove any/all "create dir" stuff for the parameter
- for deplyment
    - [ ] determine how the accounring should be done on GH
- for v2
    - [x] get an integration test
        - there's only one option
    - [ ] get .tsv files for the integration test
    - [ ] make it work with v2's folder variant
    - [ ] get v2's trino/postgresql variants working with the API interfaces
    - [x] check if v2 test(s) can be "normalised" to work like this?
        - yes?
- for fun
    - check if that "is v2" check i saw is usable
        - it is; can i merge all the functionality?
    - test and see if we/i can auto-determine the person table
        - tests indicate as such (for v1 anyway)
    - can i use just ONE validation function for person_rules_check_v2?
        - i changed v2 to ignore the .csv suffix
