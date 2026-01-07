import importlib.resources as resources
import sys
import time
from pathlib import Path

import click

import carrottransform.tools as tools
import carrottransform.tools.args as args
from carrottransform import require
from carrottransform.tools import outputs, sources
from carrottransform.tools.args import (
    OnlyOnePersonInputAllowed,
    PathArg,
    person_rules_check,
    remove_csv_extension,
)
from carrottransform.tools.args import (
    person_rules_check_v2_injected as person_rules_check_v2,
)
from carrottransform.tools.core import get_target_records
from carrottransform.tools.date_helpers import normalise_to8601
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.person_helpers import (
    load_last_used_ids,
    read_person_ids,
)
from carrottransform.tools.person_helpers import (
    load_person_ids_v2_inject as load_person_ids_v2,
)
from carrottransform.tools.record_builder import RecordBuilderFactory
from carrottransform.tools.stream_helpers import StreamingLookupCache
from carrottransform.tools.types import RecordContext

logger = logger_setup()


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


@click.command()
@args.common
def div2(
    inputs: sources.SourceObject,
    output: outputs.OutputTarget,
    rules_file: Path,
    person: str,
    omop_ddl_file: None | Path,
    omop_version: None | str,
):
    require(
        not person.endswith(".csv"),
        "don't call the person table .csv - just use the name",
    )

    logger.info("starting v2 with injected source and output")

    start_time = time.time()

    # this used to be a parameter; it's hard coded now but otherwise unchanged
    omop_config_file: Path = PathArg.convert("@carrot/config/config.json", None, None)
    require(omop_config_file.is_file())
    # default to 5.3 - value is onlu used for ddl fallback so nailing it in place
    if omop_version is None:
        omop_version = "5.3"

    #
    if omop_ddl_file is None:
        omop_ddl_file = PathArg.convert(
            f"@carrot/config/OMOPCDM_postgresql_{omop_version}_ddl.sql", None, None
        )
    assert omop_ddl_file is not None, "omopddl/omop_ddl_file musn't be null"
    require(omop_ddl_file.is_file())

    # fixes it
    RecordBuilderFactory.clear_person_cache()

    try:
        # try to open the persons
        inputs.open(person)

        """Initialize all processing components"""
        self_omopcdm = OmopCDM(omop_ddl_file, omop_config_file)

        self_mappingrules = MappingRules(rules_file, self_omopcdm)

        if not self_mappingrules.is_v2_format:
            raise ValueError("Rules file is not in v2 format!")
        else:
            try:
                person_rules_check_v2(person, self_mappingrules)
            except Exception as e:
                logger.exception(f"Validation for person rules failed: {e}")
                raise e

        # Pre-compute lookup cache for efficient streaming
        self_lookup_cache = StreamingLookupCache(self_mappingrules, self_omopcdm)

        logger.info("v2 assumes it is always `person_ids` and doesn't try to reuse ids")

        self_metrics = tools.metrics.Metrics(self_mappingrules.get_dataset_name())

        # compute the "real id" to the "anon id" mapping
        person_lookup, rejected_person_count = load_person_ids_v2(
            mappingrules=self_mappingrules,
            inputs=inputs,
            person=person,
        )

        # now save the IDs
        id_out = output.start("person_ids", ["SOURCE_SUBJECT", "TARGET_SUBJECT"])

        for person_source_id, person_assigned_id in person_lookup.items():
            id_out.write([person_source_id, person_assigned_id])

        id_out.close()

        # Log results of person lookup
        logger.info(
            f"person_id stats: total loaded {len(person_lookup)}, reject count {rejected_person_count}"
        )

        # Setup output files - keep all open for streaming
        output_files = self_mappingrules.get_all_outfile_names()
        target_column_maps = {}
        file_handles = {}
        for output_name in output_files:
            output_header = self_omopcdm.get_omop_column_list(output_name)
            target_column_map = self_omopcdm.get_omop_column_map(output_name)

            if output_header is None:
                raise Exception(f"need columns for {output_name=}")
            if target_column_map is None:
                raise Exception(f"need column map for {output_name=}")

            file_handles[output_name] = output.start(output_name, output_header)
            target_column_maps[output_name] = target_column_map

        record_numbers = {output_file: 1 for output_file in output_files}

        logger.info("Processing data...")
        total_output_counts = {outfile: 0 for outfile in output_files}
        input_files = self_mappingrules.get_all_infile_names()
        total_rejected_counts = {infile: 0 for infile in input_files}

        # --------------------------------------------------------------------------
        # INLINE former _process_input_file_stream22()
        # --------------------------------------------------------------------------
        for source_filename in input_files:
            try:
                source = inputs.open(remove_csv_extension(source_filename))

                logger.info(f"Streaming input file: {source_filename}")

                applicable_targets: set = self_lookup_cache.input_to_outputs.get(
                    source_filename, set()
                )
                logger.info(f"{applicable_targets=}")
                if not applicable_targets:
                    logger.info(f"No mappings found for {source_filename}")
                    output_counts = {}
                    rejected_count = 0
                else:
                    output_counts = {target: 0 for target in applicable_targets}
                    rejected_count = 0

                    file_meta = self_lookup_cache.file_metadata_cache[source_filename]
                    if (
                        not file_meta["datetime_source"]
                        or not file_meta["person_id_source"]
                    ):
                        logger.warning(
                            f"Missing date or person ID mapping for {source_filename}"
                        )
                    else:
                        try:
                            column_headers = next(source)
                            input_column_map = self_omopcdm.get_column_map(
                                column_headers
                            )

                            datetime_col_idx = input_column_map.get(
                                file_meta["datetime_source"]
                            )
                            if datetime_col_idx is None:
                                logger.warning(
                                    f"Date field {file_meta['datetime_source']} not found in {source_filename}"
                                )
                            else:
                                # Stream process each row directly (inlined)
                                for input_data in source:
                                    # Increment input count
                                    self_metrics.increment_key_count(
                                        source=source_filename,
                                        fieldname="all",
                                        tablename="all",
                                        concept_id="all",
                                        additional="",
                                        count_type="input_count",
                                    )

                                    # Normalize date
                                    fulldate = normalise_to8601(
                                        input_data[datetime_col_idx]
                                    )
                                    if fulldate is None:
                                        self_metrics.increment_key_count(
                                            source=source_filename,
                                            fieldname="all",
                                            tablename="all",
                                            concept_id="all",
                                            additional="",
                                            count_type="input_date_fields",
                                        )
                                        rejected_count += 1
                                        continue

                                    input_data[datetime_col_idx] = fulldate

                                    # Process per target
                                    for target_file in applicable_targets:
                                        v2_mapping = self_mappingrules.v2_mappings[
                                            target_file
                                        ][source_filename]
                                        target_column_map = target_column_maps[
                                            target_file
                                        ]

                                        target_meta = (
                                            self_lookup_cache.target_metadata_cache[
                                                target_file
                                            ]
                                        )
                                        auto_num_col = target_meta["auto_num_col"]
                                        person_id_col = target_meta["person_id_col"]
                                        date_col_data = target_meta["date_col_data"]
                                        date_component_data = target_meta[
                                            "date_component_data"
                                        ]
                                        notnull_numeric_fields = target_meta[
                                            "notnull_numeric_fields"
                                        ]

                                        data_columns = file_meta["data_fields"].get(
                                            target_file, []
                                        )

                                        output_count_local = 0
                                        rejected_count_local = 0

                                        for data_column in data_columns:
                                            if data_column not in input_column_map:
                                                continue

                                            # Build the record directly
                                            builder = RecordBuilderFactory.create_builder(
                                                RecordContext(
                                                    tgtfilename=target_file,
                                                    tgtcolmap=target_column_map,
                                                    v2_mapping=v2_mapping,
                                                    srcfield=data_column,
                                                    srcdata=input_data,
                                                    srccolmap=input_column_map,
                                                    srcfilename=source_filename,
                                                    omopcdm=self_omopcdm,
                                                    metrics=self_metrics,
                                                    person_lookup=person_lookup,
                                                    record_numbers=record_numbers,
                                                    file_handles=file_handles,
                                                    auto_num_col=auto_num_col,
                                                    person_id_col=person_id_col,
                                                    date_col_data=date_col_data,
                                                    date_component_data=date_component_data,
                                                    notnull_numeric_fields=notnull_numeric_fields,
                                                )
                                            )
                                            result_obj = builder.build_records()

                                            self_metrics = result_obj.metrics

                                            if not result_obj.success:
                                                rejected_count_local += 1

                                            output_count_local += (
                                                result_obj.record_count
                                            )

                                        output_counts[target_file] += output_count_local
                                        rejected_count += rejected_count_local

                        except Exception as e:
                            logger.error(
                                f"Error streaming file {source_filename}: {str(e)}"
                            )
                            raise

                # Update totals
                for target_file, count in output_counts.items():
                    total_output_counts[target_file] += count
                total_rejected_counts[source_filename] = rejected_count

            except Exception as e:
                logger.error(f"Error processing file {source_filename}: {str(e)}")
                raise

        # --------------------------------------------------------------------------

        # write outputs
        for target_file, count in total_output_counts.items():
            logger.info(f"TARGET: {target_file}: output count {count}")

        # Write summary
        data_summary = None
        for line in self_metrics.get_mapstream_summary().strip().split("\n"):
            row = line.split("\t")

            if data_summary is None:
                data_summary = output.start("summary_mapstream", row)
            else:
                data_summary.write(row)

        require(data_summary is not None)
        assert data_summary is not None
        data_summary.close()

        # close/flush these because we need the files on-disk for unit test valiation
        output.close()
        inputs.close()

        logger.info(
            f"V2 processing completed successfully in {time.time() - start_time:.5f} secs"
        )

    except Exception as e:
        import traceback

        # Get the full stack trace as a string
        stack_trace = traceback.format_exc()
        # Write stack trace to file
        trace = Path("trace.txt").absolute()
        with trace.open("a") as f:
            f.write(f"Error occurred: {str(e)}\n")
            f.write("Full stack trace:\n")
            f.write(stack_trace)
            f.write("\n" + "=" * 50 + "\n")  # separator for multiple errors

        logger.error(f"V2 processing failed with error: {str(e)} (added to {trace=})")
        raise


@click.group(help="Commands for mapping data to the OMOP CommonDataModel (CDM).")
def run():
    pass


run.add_command(mapstream, "mapstream")

# should let the user use "v1" or "v2" to run the command
run.add_command(mapstream, "v1")
run.add_command(div2, "v2")

if __name__ == "__main__":
    run()
