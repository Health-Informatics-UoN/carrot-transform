
if __name__ == '__main__':


    def enblank(path):
        import os, shutil
        if path.is_dir():
            shutil.rmtree(path)

        os.makedirs(path, exist_ok=True)
        return path

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
    ##
    # 
    import json
    with open(csv_files[0].parent / 'rules.json', 'r') as file:
        rules = json.load(file)

    
    for r in find_rule_names(rules):
        print(r)

    def filtrer(tbale, rule):
        # if rule == 'Body mass index 971643':
        #     return True
        # return 'MALE 971661' == rule # e = KeyError('e_1st_bday')
        # return tbale != 'person' or 'FEMALE 971662' == rule # e = KeyError('e_1st_bday')
        if 'MALE 971663' == rule : return True # e = KeyError('e_dob')

        # return tbale != 'person' or 'FEMALE 971664' == rule # e = KeyError('e_dob')

        return False
        
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
                expect_error=True,
            )
        )
        print(result.exception)
        message += f"\n\t{person_file.name}, r = {result.exit_code},\n\t\te = {result.exception!r}"






    ##
    # check the results
    raise Exception(message)
    