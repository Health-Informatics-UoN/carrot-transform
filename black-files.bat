@ECHO OFF

: simple file to keep known "new" files "up to snuff" with the black tool



black tests/test_end_to_end.py
black tests/test_rules_json.py
black carrottransform/tools/click.py
