
delete this before merging with main

this branch includes the functionality to write data to s3 buckets intest of csv files.
oddly; these are pretty simmilar.

to switch between the two, we/i change the "output-dir" arg to now specify either a path to where the output should be, or, an `s3:<bucket name>` value.
from there the arg type (thing) instantiates something to write the tables to.

- for v1
    - [ ] read/verify the results of the test
    - [ ] change `output-dir` to just `output`
    - [ ] remove any/all "create dir" stuff for the parameter
    - [ ] set this up for CI testing on GitHub
- for merge
    - [ ] make it work with v2
