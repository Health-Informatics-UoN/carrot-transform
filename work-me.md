
delete this before merging with main

this branch includes the functionality to write data to s3 buckets intest of csv files.
oddly; these are pretty simmilar.

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
    - [ ] cleanup the logic between old and new tests

- for v1
    - [x] change `output-dir` to just `output`
    - [ ] remove any/all "create dir" stuff for the parameter
    - [ ] set this up for CI testing on GitHub
- for merge
    - [ ] make it work with v2
