import sys
import time
from pathlib import Path
import click

import carrottransform.tools as tools
from carrottransform.tools.click import PathArgs
from carrottransform.tools.file_helpers import (
    check_dir_isvalid,
    check_files_in_rules_exist,
    open_file,
    resolve_paths,
    set_omop_filenames,
)
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.core import (
    get_target_records,
)
from carrottransform.tools.date_helpers import normalise_to8601
from carrottransform.tools.person_helpers import (
    load_last_used_ids,
    load_person_ids,
    set_saved_person_id_file,
)
from carrottransform.tools.args import person_rules_check, OnlyOnePersonInputAllowed

logger = logger_setup()


@click.command()
@click.option(
    "--rules-file",
    type=PathArgs,
    required=True,
    help="json file containing mapping rules",
)
@click.option(
    "--output-dir",
    type=PathArgs,
    default=None,
    required=True,
    help="define the output directory for OMOP-format tsv files",
)
@click.option(
    "--write-mode",
    default="w",
    type=click.Choice(["w", "a"]),
    help="force write-mode on output files",
)
@click.option(
    "--person-file",
    type=PathArgs,
    required=True,
    help="File containing person_ids in the first column",
)
@click.option(
    "--omop-ddl-file",
    type=PathArgs,
    required=False,
    help="File containing OHDSI ddl statements for OMOP tables",
)
@click.option(
    "--omop-config-file",
    type=PathArgs,
    required=False,
    help="File containing additional / override json config for omop outputs",
)
@click.option(
    "--omop-version",
    required=False,
    help="Quoted string containing omop version - eg '5.3'",
)
@click.option(
    "--saved-person-id-file",
    type=PathArgs,
    default=None,
    required=False,
    help="Full path to person id file used to save person_id state and share person_ids between data sets",
)
@click.option(
    "--use-input-person-ids",
    required=False,
    default="N",
    help="Use person ids as input without generating new integers",
)
@click.option(
    "--last-used-ids-file",
    type=PathArgs,
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
@click.option("--input-dir", type=PathArgs, required=True, help="Input directories")
def mapstream(
    rules_file: Path,
    output_dir: Path,
    write_mode,
    person_file: Path,
    omop_ddl_file: Path,
    omop_config_file: Path,
    omop_version,
    saved_person_id_file: Path,
    use_input_person_ids,
    last_used_ids_file: Path,
    log_file_threshold,
    input_dir: Path,
):
    """
    Map to output using input streams
    """

    # Resolve any @package paths in the arguments
    [
        rules_file,
        output_dir,
        person_file,
        omop_ddl_file,
        omop_config_file,
        saved_person_id_file,
        last_used_ids_file,
        input_dir,
    ] = resolve_paths(
        [
            rules_file,
            output_dir,
            person_file,
            omop_ddl_file,
            omop_config_file,
            saved_person_id_file,
            last_used_ids_file,
            input_dir,
        ]
    )

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
                    output_dir,
                    write_mode,
                    person_file,
                    omop_ddl_file,
                    omop_config_file,
                    omop_version,
                    saved_person_id_file,
                    use_input_person_ids,
                    last_used_ids_file,
                    log_file_threshold,
                    input_dir,
                ],
            )
        )
    )

    # check on the rules file
    if (rules_file is None) or (not rules_file.is_file()):
        logger.exception(f"rules file was set to `{rules_file=}` and is missing")
        sys.exit(-1)

    ## set omop filenames
    omop_config_file, omop_ddl_file = set_omop_filenames(
        omop_ddl_file, omop_config_file, omop_version
    )
    ## check directories are valid
    check_dir_isvalid(input_dir)  # Input directory must exist - we need the files in it
    check_dir_isvalid(
        output_dir, create_if_missing=True
    )  # Create output directory if needed

    saved_person_id_file = set_saved_person_id_file(saved_person_id_file, output_dir)

    ## check on the person_file_rules
    try:
        person_rules_check(rules_file=rules_file, person_file=person_file)
    except OnlyOnePersonInputAllowed as e:
        inputs = list(sorted(list(e._inputs)))

        logger.error(
            f"Person properties were mapped from ({inputs}) but can only come from the person file {person_file.name=}"
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
        person_lookup, rejected_person_count = load_person_ids(
            saved_person_id_file, person_file, mappingrules, use_input_person_ids
        )

        ## open person_ids output file
        with saved_person_id_file.open(mode="w") as fhpout:
            ## write the header to the file
            fhpout.write("SOURCE_SUBJECT\tTARGET_SUBJECT\n")
            ##iterate through the ids and write them to the file.
            for person_id, person_assigned_id in person_lookup.items():
                fhpout.write(f"{str(person_id)}\t{str(person_assigned_id)}\n")

        ## Initialise output files (adding them to a dict), output a header for each
        ## these aren't being closed deliberately
        for tgtfile in output_files:
            fhd[tgtfile] = (
                (output_dir / tgtfile).with_suffix(".tsv").open(mode=write_mode)
            )
            if write_mode == "w":
                outhdr = omopcdm.get_omop_column_list(tgtfile)
                fhd[tgtfile].write("\t".join(outhdr) + "\n")
            ## maps all omop columns for each file into a dict containing the column name and the index
            ## so tgtcolmaps is a dict of dicts.
            tgtcolmaps[tgtfile] = omopcdm.get_omop_column_map(tgtfile)

    except IOError as e:
        logger.exception(f"I/O - error({e.errno}): {e.strerror} -> {str(e)}")
        exit()

    logger.info(
        f"person_id stats: total loaded {len(person_lookup)}, reject count {rejected_person_count}"
    )

    ## Compare files found in the input_dir with those expected based on mapping rules
    existing_input_files = [f.name for f in input_dir.glob("*.csv")]
    rules_input_files = mappingrules.get_all_infile_names()

    ## Log mismatches but continue
    check_files_in_rules_exist(rules_input_files, existing_input_files)

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

        fhcsvr = open_file(input_dir / srcfilename)
        if fhcsvr is None:  # check if it's none before unpacking
            raise Exception(f"Couldn't find file {srcfilename} in {input_dir}")
        fh, csvr = fhcsvr  # unpack now because we can't unpack none

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
                                fhd[tgtfile].write("\t".join(outrecord) + "\n")
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

        fh.close()

        logger.info(
            f"INPUT file data : {srcfilename}: input count {str(rcount)}, time since start {time.time() - start_time:.5} secs"
        )
        for outtablename, count in outcounts.items():
            logger.info(f"TARGET: {outtablename}: output count {str(count)}")
    # END main processing loop

    logger.info(
        "--------------------------------------------------------------------------------"
    )

    data_summary = metrics.get_mapstream_summary()
    try:
        dsfh = (output_dir / "summary_mapstream.tsv").open(mode="w")
        dsfh.write(data_summary)
        dsfh.close()
    except IOError as e:
        logger.exception(f"I/O error({e.errno}): {e.strerror}")
        logger.exception("Unable to write file")
        raise e

    # END mapstream
    logger.info(f"Elapsed time = {time.time() - start_time:.5f} secs")


@click.group(help="Commands for mapping data to the OMOP CommonDataModel (CDM).")
def run():
    pass


run.add_command(mapstream, "mapstream")
if __name__ == "__main__":
    run()
