
import pytest

from pathlib import Path

import logging

import clicktools
import csvrow


import re

@pytest.mark.integration
def test_multiple_patient_files_simplified(tmp_path: Path):
    
    (result, output, person_id_source2target, person_id_target2source) = (
        clicktools.click_generic(
            tmp_path,
            'multiple_patient_files/demographics_mother_gold.csv',
            # removed most of the rules
            # changed the appropriate dates to e_dob
            rules = 'simplified.json'
        )
    )

    ##
    # check the results
    raise Exception(f'??? no entries seem to come across {tmp_path=}')

@pytest.mark.integration
def test_multiple_patient_files_fullrules(tmp_path: Path):
    
    (result, output, person_id_source2target, person_id_target2source) = (
        clicktools.click_generic(
            tmp_path,
            'multiple_patient_files/demographics_mother_gold.csv',
            # changed the appropriate dates to e_dob
            # ? TODO; fix NA?
            rules = 'fullrules.json'
        )
    )

    ##
    # check the results
    raise Exception(f'??? {tmp_path=}')

def main():

    tmp_path = Path(__file__).parent / "target" / __name__
    enblank(tmp_path)
    
    ##
    # getup the person file
    # demographics_mother is probbaly the correct one
    csv_files = list( (Path(__file__).parent / "test_data/mireda_key_error").glob("*.csv") )
    # let's just do that
    csv_files = [(Path(__file__).parent / "test_data/mireda_key_error/demographics_mother_gold.csv")]
    # csv_files = [(Path(__file__).parent / "test_data/mireda_key_error/demographics_child_gold.csv")]

    ##
    # cool - now do the rules

    ##
    # 
    import json
    with open(csv_files[0].parent / 'rules.json', 'r') as file:
        rules = json.load(file)

    
    for r in find_rule_names(rules):
        print(r)

    def filtrer(tbale, rule):
        allowed_rules = [
            'Body mass index 971643',
            'MALE 971661',
            'FEMALE 971662',
            'MALE 971663',
            'Weight 971639',
        ]

        return rule in allowed_rules
        
    simplified = tmp_path / 'simplified.json'
    with open(simplified, 'w') as f:
        json.dump(filter_rules(rules,  filtrer), f, indent=4)


    ##
    # run it a lot
    message = 'failed with person file:'
    for person_file in csv_files:

        dir = enblank(tmp_path / person_file.name)
        (result, output) = (
            clicktools.click_generic(
                dir,
                person_file,
                rules = simplified,
                failure=True,
            )
        )
        print(result.exception)
        message += f"\n\t{person_file.name}, r = {result.exit_code},\n\t\te = {result.exception!r}"






    ##
    # check the results
    raise Exception(message)

def enblank(path):
    import os, shutil
    if path.is_dir():
        shutil.rmtree(path)

    os.makedirs(path, exist_ok=True)
    return path

def find_rule_names(rules: dict):
    for section in rules:
        
        if 'cdm' != section:
            continue


        for table in rules[section]:

            for rule in rules[section][table]:
                yield rule



def filter_rules(rules: dict, keep):
    """produce a new json file with only some of the rules"""  

    simplified = {}
    for section in rules:
        
        if 'cdm' != section:
            simplified[section] = rules[section]
            continue

        simplified[section] = {}    

        for table in rules[section]:

            for rule in rules[section][table]:
                if keep(table, rule):
                    if table not in simplified[section]:
                        simplified[section][table] = {}
                    simplified[section][table][rule] = rules[section][table][rule]

    return simplified

if __name__ == '__main__':
    just_run_it()

    