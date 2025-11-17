
import logging
import re
from pathlib import Path

import boto3
import pytest
import sqlalchemy
from click.testing import CliRunner

import carrottransform.cli.subcommands.run_v2 as run_v2
import carrottransform.tools.outputs as outputs
import carrottransform.tools.sources as sources
import tests.click_tools as click_tools
import tests.csvrow as csvrow
import tests.testools as testools
from  tests.test_integration2 import test_data

logger = logging.getLogger(__name__)

##
#
from itertools import product

def filter_passes(pass_how):
    for case in pass_how:
        t_count = 0
        f_count = 0

        for v in case.values():
            if v:
                t_count += 1
            else:
                f_count += 1
        
        if 0 == t_count or 1 == t_count or 0 == f_count or 1 == f_count:
            yield case

##
# this is all the way to pass the arguments
pass_how = testools.keyed_variations(
    input = [True, False],
    output = [True, False],
    mapping = [True, False],
    person = [True, False],
    ddl = [True, False, None],
)

def env_or_cli(pass_how, **kv):
    env = {}
    cli = []

    settings = dict(kv)

    if pass_how['input']:
        env["INPUTS"] = settings['input']
    elif pass_how['input'] is not None:
       cli.append('--inputs')
       cli.append(settings['input'])

    if pass_how['output']:
        env["OUTPUT"] = settings['output']
    elif pass_how['output'] is not None:
       cli.append('--output')
       cli.append(settings['output'])

    if pass_how['mapping']:
        env["RULES_FILE"] = settings['mapping']
    elif pass_how['mapping'] is not None:
       cli.append('--rules-file')
       cli.append(settings['mapping'])


    if pass_how['person']:
        env["PERSON"] = settings['person']
    elif pass_how['person'] is not None:
       cli.append('--person')
       cli.append(settings['person'])

    if pass_how['ddl']:
        env["OMOP_DDL_FILE"] = settings['ddl']
    elif pass_how['ddl'] is not None:
       cli.append('--omop-ddl-file')
       cli.append(settings['ddl'])

    return env, cli
    

# filter it so that we ONLY keep items with just 0 or just 1 of true/false
pass_how = filter_passes(pass_how)

##
# these are all the paramter variatsions
configurations = testools.keyed_variations(
    input = ['csv', 'sqlite', f"s3:{testools.CARROT_TEST_BUCKET}"],
    output = ['csv', 'sqlite', f"s3:{testools.CARROT_TEST_BUCKET}"],
)

##
# test cases
from dataclasses import dataclass
from functools import cached_property

@dataclass(frozen=True)
class integration_test_case:
    person_path: str
    mapper_file: Path
    suffix: str
    worker: object

    @cached_property  
    def mapper(self) -> str:
        return str(self.mapper_file)
        
    @cached_property  
    def person(self) -> str:
        person = self.person_path
        if '/' in person:
            person = person[person.rfind('/')+1:]
        
        if person.endswith('.csv'):
            person = person[:-4]

        return person

    @cached_property
    def folder(self) -> Path:
        return (test_data / self.person_path).parent


import carrottransform.cli.subcommands.run_v2
import carrottransform.cli.subcommands.run

test_cases = [
    integration_test_case(
        person_path="integration_test1/src_PERSON.csv",
        mapper_file=test_data.parent / "test_V2/rules-v2.json",
        suffix= "v2-out/",
        worker= carrottransform.cli.subcommands.run_v2.folder,
    ),
    
    integration_test_case(
        person_path="integration_test1/src_PERSON.csv",
        mapper_file=test_data / "integration_test1/transform-rules.json",
        suffix= "",
        worker= carrottransform.cli.subcommands.run.mapstream
    ),
]


subject = testools.keyed_variations(
    test_case= test_cases,
    configuration = configurations,
)



##
#
count = 0
@pytest.mark.parametrize("pass_how, subject", testools.zip_long(
    pass_how,
    testools.keyed_variations(
        test_case= test_cases,
        configuration = [c for c in configurations if (not c['output'].startswith('s3:')) and (not c['input'].startswith('s3:'))   ],
    )
))
@pytest.mark.integration
def test_integration(request, tmp_path: Path, subject, pass_how):
    global count
    logger.info(f'yesum {count=}')
    count+=1

    test_case: integration_test_case = subject['test_case']
    configuration: dict[str, str] = subject['configuration']

    ##
    # establish test configuration
    output_to: str = configuration['output']
    input_from: str = configuration['input']
    main_entry = test_case.worker
    test_suffix: str =  test_case.suffix


    # generate a semi-random slug/name to group test data under
    # the files we read/write to s3 will appear in this folder
    import re
    slug = (
        re.sub(r"[^a-zA-Z0-9]+", "_", request.node.name).strip("_")
        + "__"
        + testools.rand_hex()
    )


    # set the input
    inputs: None | str = None
    if "sqlite" == input_from:
        inputs = test_case.load_sqlite(tmp_path)

    if "csv" == input_from:
        inputs = str(test_case.folder).replace("\\", "/")
        
    if input_from.startswith("s3:"):
        # create a random s3 subfolder
        inputs = input_from + "/" + slug + "/input"

        # set a task to delete the subfolder on exit
        request.addfinalizer(lambda: testools.delete_s3_folder(inputs))

        # copy data into the thing
        outputTarget = outputs.s3OutputTarget(inputs)
        testools.copy_across(ot=outputTarget, so=test_case.folder, names=None)
    assert inputs is not None, f"couldn't use {input_from=}"  # check inputs as set

    # set the output
    output: None | str = None
    if "csv" == output_to:
        output = str((tmp_path / "out").absolute())
    if "sqlite" == output_to:
        output = f"sqlite:///{(tmp_path / 'output.sqlite3').absolute()}"

    if output_to.startswith("s3:"):
        # create a random s3 subfolder
        output = output_to + "/" + slug + "/output"

        # set a task to delete the subfolder on exit
        request.addfinalizer(lambda: testools.delete_s3_folder(output))
    assert output is not None, f"couldn't use {output_to=}"  # check output was set


    # set the cli args and envars
    env, cli = env_or_cli(
        pass_how,
        input = inputs,
        output = output,
        mapping = test_case.mapper,
        person = test_case.person,
        ddl = "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
    )

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(main_entry, args=cli, env=env)

    if result.exception is not None:
        logger.error(result.exception)
        raise (result.exception)

    assert 0 == result.exit_code

    raise Exception('need to check results')
    # raise Exception()

