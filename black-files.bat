@ECHO OFF

: simple file to keep known "new" files "up to snuff" with the black tool



black carrottransform/tools/args.py
black carrottransform/tools/click.py

black tests/test_args.py
black tests/test_end_to_end.py
black tests/test_run.py


