import importlib.resources as resources
import sys
import time
from pathlib import Path

import click
from sqlalchemy.engine import Engine

import carrottransform.tools as tools
import carrottransform.tools.sources as sources
from carrottransform import require
from carrottransform.tools import outputs
from carrottransform.tools.args import (
    AlchemyConnectionArg,
    OnlyOnePersonInputAllowed,
    PathArg,
    PatternStringParamType,
    person_rules_check,
    remove_csv_extension,
)
from carrottransform.tools.core import get_target_records
from carrottransform.tools.date_helpers import normalise_to8601
from carrottransform.tools.file_helpers import (
    check_dir_isvalid,
)
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.person_helpers import (
    load_last_used_ids,
    read_person_ids,
)

logger = logger_setup()

import carrottransform.tools.args as args


@click.command()
@args.common
@click.option(
    "--use-input-person-ids",
    required=False,
    default="N",
    help="Use person ids as input without generating new integers",
)
@click.option(
    "--last-used-ids-file",
    type=PathArg,
    default=None,
    required=False,
    help="Full path to last used ids file for OMOP tables - format: tablename\tlast_used_id, \nwhere last_used_id must be an integer",
)
@click.option(
    "--log-file-threshold",
    required=False,
    default=0,
    help="Lower outcount limit for logfile output",
)
def mapstream(
    rules_file: Path,
    person: str,
    inputs: sources.SourceObject,
    output: outputs.OutputTarget,
    omop_ddl_file: Path | None,
    omop_version,
    use_input_person_ids,
    last_used_ids_file: Path | None,
    log_file_threshold,
):
    # the write-mode needs to be reimplemented
    write_mode: str = "w"

    """
    Map to output using input streams
    """

    # this used to be a parameter; it's hard coded now but otherwise unchanged
    omop_config_file: Path = PathArg.convert("@carrot/config/config.json", None, None)

    # Initialisation
    # - check for values in optional arguments
    # - read in configuration files
    # - check main directories for existence
    # - handle saved person ids
    # - initialise metrics
    logger.info(
        ",".join(
            map(
                str,
                [
                    rules_file,
                    write_mode,
                    omop_ddl_file,
                    omop_config_file,
                    omop_version,
                    use_input_person_ids,
                    last_used_ids_file,
                    log_file_threshold,
                ],
            )
        )
    )

    # check on the rules file
    if (rules_file is None) or (not rules_file.is_file()):
        logger.error(f"rules file was set to {rules_file=} and is missing")
        sys.exit(-1)

    ## fallback for the ddl filename
    if omop_ddl_file is None:
        omop_ddl_name = f"OMOPCDM_postgresql_{omop_version}_ddl.sql"
        omop_ddl_file = Path(
            Path(str(resources.files("carrottransform"))) / "config" / omop_ddl_name
        )
        if not omop_ddl_file.is_file():
            logger.warning(f"{omop_ddl_name=} not found")

    ## check on the person_file_rules
    try:
        person_rules_check(rules_file=rules_file, person_file_name=person)
    except OnlyOnePersonInputAllowed as e:
        input_list = list(sorted(list(e._inputs)))

        logger.error(
            f"Person properties were mapped from ({input_list}) but can only come from the person file {person=}"
        )
        sys.exit(-1)
    except Exception as e:
        logger.exception(f"person_file_rules check failed: {e}")
        sys.exit(-1)

    start_time = time.time()
    ## create OmopCDM object, which contains attributes and methods for the omop data tables.
    omopcdm = tools.omopcdm.OmopCDM(omop_ddl_file, omop_config_file)

    ## mapping rules determine the ouput files? which input files and fields in the source data, AND the mappings to omop concepts
    mappingrules = tools.mappingrules.MappingRules(rules_file, omopcdm)
    metrics = tools.metrics.Metrics(mappingrules.get_dataset_name(), log_file_threshold)

    logger.info(
        "--------------------------------------------------------------------------------"
    )
    logger.info(
        f"Loaded mapping rules from: {rules_file} in {time.time() - start_time:.5f} secs"
    )

    output_files = mappingrules.get_all_outfile_names()

    ## set record number
    ## will keep track of the current record number in each file, e.g., measurement_id, observation_id.
    record_numbers = {}
    for output_file in output_files:
        record_numbers[output_file] = 1
    if (last_used_ids_file is not None) and last_used_ids_file.is_file():
        record_numbers = load_last_used_ids(last_used_ids_file, record_numbers)

    fhd = {}
    tgtcolmaps = {}

    try:
        ## get all person_ids from file and either renumber with an int or take directly, and add to a dict
        person_lookup, rejected_person_count = read_person_ids(
            # this is a little horrible; i'm not ready to rewrite/replace `read_person_ids()` so we just do this pointeing to a fake file
            Path(__file__) / "this-should-not-exist.txt",
            inputs.open(remove_csv_extension(person)),
            mappingrules,
            use_input_person_ids != "N",
        )

        ## open person_ids output file with a header
        fhpout = output.start("person_ids", ["SOURCE_SUBJECT", "TARGET_SUBJECT"])

        ## write the id pair to a file or table
        for person_id, person_assigned_id in person_lookup.items():
            fhpout.write([str(person_id), str(person_assigned_id)])
        fhpout.close()

        ## Initialise output files (adding them to a dict), output a header for each
        ## these aren't being closed deliberately
        for target_file in output_files:
            # if write_mode == "w":
            out_header = omopcdm.get_omop_column_list(target_file)

            fhd[target_file] = output.start(target_file, out_header)

            ## maps all omop columns for each file into a dict containing the column name and the index
            ## so tgtcolmaps is a dict of dicts.
            tgtcolmaps[target_file] = omopcdm.get_omop_column_map(target_file)

    except IOError as e:
        logger.exception(f"I/O - error({e.errno}): {e.strerror} -> {str(e)}")
        sys.exit(-1)

    logger.info(
        f"person_id stats: total loaded {len(person_lookup)}, reject count {rejected_person_count}"
    )

    rules_input_files = mappingrules.get_all_infile_names()

    ## set up overall counts
    rejidcounts = {}
    rejdatecounts = {}
    logger.info(rules_input_files)

    ## set up per-input counts
    for srcfilename in rules_input_files:
        rejidcounts[srcfilename] = 0
        rejdatecounts[srcfilename] = 0

    ## main processing loop, for each input file
    for srcfilename in rules_input_files:
        rcount = 0

        csvr = inputs.open(remove_csv_extension(srcfilename))

        ## create dict for input file, giving the data and output file
        tgtfiles, src_to_tgt = mappingrules.parse_rules_src_to_tgt(srcfilename)
        infile_datetime_source, infile_person_id_source = (
            mappingrules.get_infile_date_person_id(srcfilename)
        )

        outcounts = {}
        rejcounts = {}
        for tgtfile in tgtfiles:
            outcounts[tgtfile] = 0
            rejcounts[tgtfile] = 0

        datacolsall = []
        csv_column_headers = next(csvr)
        dflist = mappingrules.get_infile_data_fields(srcfilename)
        for colname in csv_column_headers:
            datacolsall.append(colname)
        inputcolmap = omopcdm.get_column_map(csv_column_headers)
        pers_id_col = inputcolmap[infile_person_id_source]
        datetime_col = inputcolmap[infile_datetime_source]

        logger.info(
            "--------------------------------------------------------------------------------"
        )
        logger.info(f"Processing input: {srcfilename}")

        # for each input record
        for indata in csvr:
            metrics.increment_key_count(
                source=srcfilename,
                fieldname="all",
                tablename="all",
                concept_id="all",
                additional="",
                count_type="input_count",
            )
            rcount += 1

            # if there is a date, parse it - read it is a string and convert to YYYY-MM-DD HH:MM:SS
            fulldate = normalise_to8601(indata[datetime_col])
            if fulldate is not None:
                indata[datetime_col] = fulldate
            else:
                metrics.increment_key_count(
                    source=srcfilename,
                    fieldname="all",
                    tablename="all",
                    concept_id="all",
                    additional="",
                    count_type="input_date_fields",
                )
                continue

            for tgtfile in tgtfiles:
                tgtcolmap = tgtcolmaps[tgtfile]
                auto_num_col = omopcdm.get_omop_auto_number_field(tgtfile)
                pers_id_col = omopcdm.get_omop_person_id_field(tgtfile)

                datacols = datacolsall
                if tgtfile in dflist:
                    datacols = dflist[tgtfile]

                for datacol in datacols:
                    built_records, outrecords, metrics = get_target_records(
                        tgtfile,
                        tgtcolmap,
                        src_to_tgt,
                        datacol,
                        indata,
                        inputcolmap,
                        srcfilename,
                        omopcdm,
                        metrics,
                    )

                    if built_records:
                        for outrecord in outrecords:
                            if auto_num_col is not None:
                                outrecord[tgtcolmap[auto_num_col]] = str(
                                    record_numbers[tgtfile]
                                )
                                ### most of the rest of this section is actually to do with metrics
                                record_numbers[tgtfile] += 1

                            if (outrecord[tgtcolmap[pers_id_col]]) in person_lookup:
                                outrecord[tgtcolmap[pers_id_col]] = person_lookup[
                                    outrecord[tgtcolmap[pers_id_col]]
                                ]
                                outcounts[tgtfile] += 1

                                metrics.increment_with_datacol(
                                    source_path=srcfilename,
                                    target_file=tgtfile,
                                    datacol=datacol,
                                    out_record=outrecord,
                                )

                                # write the line to the file
                                fhd[tgtfile].write(outrecord)
                            else:
                                metrics.increment_key_count(
                                    source=srcfilename,
                                    fieldname="all",
                                    tablename=tgtfile,
                                    concept_id="all",
                                    additional="",
                                    count_type="invalid_person_ids",
                                )
                                rejidcounts[srcfilename] += 1

                    if tgtfile == "person":
                        break

        logger.info(
            f"INPUT file data : {srcfilename}: input count {rcount}, time since start {time.time() - start_time:.5} secs"
        )
        for outtablename, count in outcounts.items():
            logger.info(f"TARGET: {outtablename}: output count {count}")
    # END main processing loop

    logger.info(
        "--------------------------------------------------------------------------------"
    )

    data_summary = metrics.get_mapstream_summary()
    try:
        # convert the data into like-csv lines
        csv_like_lines = map(lambda x: x.split("\t"), data_summary.split("\n")[:-1])

        # loop through the lines writing them
        summary: None | outputs.OutputTarget.Handle = None
        for line in csv_like_lines:
            if summary is None:
                # we need the column names to "open" this sort of file/table, and, that'll be the first line
                summary = output.start("summary_mapstream", line)
            else:
                # once the summary is open
                summary.write(line)

        # mypy needs a typecheck
        if summary is not None:
            summary.close()
            summary = None
    except IOError as e:
        logger.exception(f"I/O error({e.errno}): {e.strerror}")
        logger.exception("Unable to write file")
        raise e
    output.close()

    # END mapstream
    logger.info(f"Elapsed time = {time.time() - start_time:.5f} secs")


@click.group(help="Commands for mapping data to the OMOP CommonDataModel (CDM).")
def run():
    pass


run.add_command(mapstream, "mapstream")
if __name__ == "__main__":
    run()
