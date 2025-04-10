
from carrottransform.tools.click import PathArgs
from carrottransform.tools.omopcdm import OmopCDM
from pathlib import Path
from typing import Iterator, IO
import carrottransform
import carrottransform.tools as tools
import click
import csv
import datetime
import fnmatch
import importlib.resources
import json
import logging
import os
import sys
import time

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

@click.group(help="Commands for mapping data to the OMOP CommonDataModel (CDM).")
def run():
    pass


@click.command()
@click.option("--rules-file", type=PathArgs,
              required=True,
              help="json file containing mapping rules")
@click.option("--output-dir", type=PathArgs,
              default=None,
              help="define the output directory for OMOP-format tsv files")
@click.option("--write-mode",
              default='w',
              type=click.Choice(['w','a']),
              help="force write-mode on output files")
@click.option("--person-file", type=PathArgs,
              required=True,
              help="File containing person_ids in the first column")
@click.option("--omop-ddl-file", type=PathArgs,
              required=False,
              help="File containing OHDSI ddl statements for OMOP tables")
@click.option("--omop-config-file", type=PathArgs,
              required=False,
              help="File containing additional / override json config for omop outputs")
@click.option("--omop-version",
              required=False,
              help="Quoted string containing omop version - eg '5.3'")
@click.option("--saved-person-id-file", type=PathArgs,
              default=None,
              required=False,
              help="Full path to person id file used to save person_id state and share person_ids between data sets")
@click.option("--use-input-person-ids",
              required=False,
              default='N',
              help="Use person ids as input without generating new integers")
@click.option("--last-used-ids-file", type=PathArgs,
              default=None,
              required=False,
              help="Full path to last used ids file for OMOP tables - format: tablename\tlast_used_id, \nwhere last_used_id must be an integer")
@click.option("--log-file-threshold",
              required=False,
              default=0,
              help="Lower outcount limit for logfile output")
@click.argument("input-dir", type=PathArgs, required=False, nargs=-1)
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

    ## set omop filenames
    omop_config_file, omop_ddl_file = set_omop_filenames(
        omop_ddl_file, omop_config_file, omop_version
    )
    ## check directories are valid
    check_dir_isvalid(input_dir)
    check_dir_isvalid(output_dir)

    saved_person_id_file = set_saved_person_id_file(saved_person_id_file, output_dir)

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
        person_lookup, rejected_person_count = load_person_ids(saved_person_id_file,
                                                               person_file, mappingrules,
                                                               use_input_person_ids)
        ## open person_ids output file
        with open(saved_person_id_file, mode="w") as fhpout:
            ## write the header to the file
            fhpout.write("SOURCE_SUBJECT\tTARGET_SUBJECT\n")
            ##iterate through the ids and write them to the file.
            for person_id, person_assigned_id in person_lookup.items():
                fhpout.write(f"{str(person_id)}\t{str(person_assigned_id)}")

        ## Initialise output files (adding them to a dict), output a header for each
        ## these aren't being closed deliberately
        for tgtfile in output_files:
            fhd[tgtfile] = open(output_dir / (tgtfile + ".tsv"), mode=write_mode)
            if write_mode == "w":
                outhdr = omopcdm.get_omop_column_list(tgtfile)
                fhd[tgtfile].write("\t".join(outhdr) + "\n")
            ## maps all omop columns for each file into a dict containing the column name and the index
            ## so tgtcolmaps is a dict of dicts.
            tgtcolmaps[tgtfile] = omopcdm.get_omop_column_map(tgtfile)

    except IOError as e:
        logger.exception(f"I/O - error({e.errno}): {e.strerror} -> {str(e)}")
        exit()

    logger.info(f"person_id stats: total loaded {len(person_lookup)}, reject count {rejected_person_count}")

    ## Compare files found in the input_dir with those expected based on mapping rules
    existing_input_files = fnmatch.filter(os.listdir(input_dir[0]), "*.csv")
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
        outcounts = {}
        rejcounts = {}
        rcount = 0

        fh, csvr = open_file(input_dir[0] / srcfilename)
        if fh is None:
            continue


        ## create dict for input file, giving the data and output file
        tgtfiles, src_to_tgt = mappingrules.parse_rules_src_to_tgt(srcfilename)
        infile_datetime_source, infile_person_id_source = mappingrules.get_infile_date_person_id(srcfilename)
        for tgtfile in tgtfiles:
            outcounts[tgtfile] = 0
            rejcounts[tgtfile] = 0
        datacolsall = []
        hdrdata = next(csvr)
        dflist = mappingrules.get_infile_data_fields(srcfilename)
        for colname in hdrdata:
            datacolsall.append(colname)
        inputcolmap = omopcdm.get_column_map(hdrdata)
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
                    count_type="input_count"
                )
            rcount += 1
            # if there is a date, parse it - read it is a string and convert to YYYY-MM-DD
            strdate = indata[datetime_col].split(" ")[0]
            fulldate = parse_date(strdate)
            if fulldate is not None:
                indata[datetime_col] = fulldate
            else:
                metrics.increment_key_count(
                        source=srcfilename,
                        fieldname="all",
                        tablename="all",
                        concept_id="all",
                        additional="",
                        count_type="input_date_fields"
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
                    built_records, outrecords, metrics = get_target_records(tgtfile, tgtcolmap, src_to_tgt, datacol, indata, inputcolmap, srcfilename, omopcdm, metrics)
                    if built_records:
                        for outrecord in outrecords:
                            if auto_num_col is not None:
                                outrecord[tgtcolmap[auto_num_col]] = str(record_numbers[tgtfile])
                                ### most of the rest of this section is actually to do with metrics
                                record_numbers[tgtfile] += 1
                            if (outrecord[tgtcolmap[pers_id_col]]) in person_lookup:
                                outrecord[tgtcolmap[pers_id_col]] = person_lookup[outrecord[tgtcolmap[pers_id_col]]]
                                outcounts[tgtfile] += 1

                                metrics.increment_with_datacol(
                                        source_path=srcfilename,
                                        target_file=tgtfile,
                                        datacol=datacol,
                                        out_record=outrecord
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

        fh.close()

        logger.info(f"INPUT file data : {srcfilename}: input count {str(rcount)}, time since start {time.time() - start_time:.5} secs")
        for outtablename, count in outcounts.items():
            logger.info(f"TARGET: {outtablename}: output count {str(count)}")
    # END main processing loop

    logger.info(
        "--------------------------------------------------------------------------------"
    )
    
    data_summary = metrics.get_mapstream_summary()
    try:
        dsfh = open(output_dir / "summary_mapstream.tsv", mode="w")
        dsfh.write(data_summary)
        dsfh.close()
    except IOError as e:
        logger.exception(f"I/O error({e.errno}): {e.strerror}")
        logger.exception("Unable to write file")
        raise e

    # END mapstream
    logger.info(f"Elapsed time = {time.time() - start_time:.5f} secs")


def get_target_records(
        tgtfilename: str,
        tgtcolmap: dict[str, dict[str, int]],
        rulesmap: dict[str, list[dict[str, list[str]]]],
        srcfield: str,
        srcdata: list[str],
        srccolmap: dict[str, int],
        srcfilename: str,
        omopcdm: OmopCDM,
        metrics: tools.metrics.Metrics) -> tuple[bool, list[str], tools.metrics.Metrics]:
    """
    build all target records for a given input field
    """
    build_records = False
    tgtrecords = []
    date_col_data = omopcdm.get_omop_datetime_linked_fields(tgtfilename)
    date_component_data = omopcdm.get_omop_date_field_components(tgtfilename)
    notnull_numeric_fields = omopcdm.get_omop_notnull_numeric_fields(tgtfilename)

    srckey = f"{srcfilename}~{srcfield}~{tgtfilename}"
    summarykey = srckey + "~all~"
    if valid_value(str(srcdata[srccolmap[srcfield]])):
        ## check if either or both of the srckey and summarykey are in the rules
        srcfullkey = srcfilename + "~" + srcfield + "~" + str(srcdata[srccolmap[srcfield]]) + "~" + tgtfilename
        dictkeys = []
        if srcfullkey in rulesmap:
            build_records = True
            dictkeys.append(srcfullkey)
        if srckey in rulesmap:
            build_records = True
            dictkeys.append(srckey)
        if build_records:
            for dictkey in dictkeys:
                for out_data_elem in rulesmap[dictkey]:
                    valid_data_elem = True
                    ## create empty list to store the data. Populate numerical data elements with 0 instead of empty string.
                    tgtarray = ['']*len(tgtcolmap)
                    for req_integer in notnull_numeric_fields:
                        tgtarray[tgtcolmap[req_integer]] = "0"
                    for infield, outfield_list in out_data_elem.items():
                        for output_col_data in outfield_list:
                            if "~" in output_col_data:
                                outcol, term = output_col_data.split("~")
                                tgtarray[tgtcolmap[outcol]] = term
                            else:
                                tgtarray[tgtcolmap[output_col_data]] = srcdata[srccolmap[infield]]
                            if output_col_data in date_component_data:
                                ## parse the date and store it in the proper format
                                strdate = srcdata[srccolmap[infield]].split(" ")[0]
                                dt = get_datetime_value(strdate)
                                if dt != None:
                                    year_field = date_component_data[output_col_data]["year"]
                                    month_field = date_component_data[output_col_data]["month"]
                                    day_field = date_component_data[output_col_data]["day"]
                                    tgtarray[tgtcolmap[year_field]] = str(dt.year)
                                    tgtarray[tgtcolmap[month_field]] = str(dt.month)
                                    tgtarray[tgtcolmap[day_field]] = str(dt.day)
                                    fulldate = "{0}-{1:02}-{2:02}".format(dt.year, dt.month, dt.day)
                                    tgtarray[tgtcolmap[output_col_data]] = fulldate
                                else:
                                    metrics.increment_key_count(
                                            source=srcfilename,
                                            fieldname=srcfield,
                                            tablename=tgtfilename,
                                            concept_id="all",
                                            additional="",
                                            count_type="invalid_date_fields"
                                            )
                                    valid_data_elem = False
                            elif output_col_data in date_col_data:
                                fulldate = srcdata[srccolmap[infield]]
                                tgtarray[tgtcolmap[output_col_data]] = fulldate
                                tgtarray[tgtcolmap[date_col_data[output_col_data]]] = fulldate
                    if valid_data_elem:
                        tgtrecords.append(tgtarray)
    else:
        metrics.increment_key_count(
                source=srcfilename,
                fieldname=srcfield,
                tablename=tgtfilename,
                concept_id="all",
                additional="",
                count_type="invalid_source_fields"
            )

    return build_records, tgtrecords, metrics


def valid_value(item):
    """
    Check if an item is non blank (null)
    """
    if item.strip() == "":
        return False
    return True


# DATE TESTING
# ------------
# I started by changing the get_datetime_value to be neater.
# I think it should be handled all as one thing, but I've spent too much time doing this already

def valid_date_value(item):
    """
    Check if a date item is non null and parses as ISO (YYYY-MM-DD), reverse-ISO
    or dd/mm/yyyy or mm/dd/yyyy
    """
    if item.strip() == "":
        return(False)
    if not valid_iso_date(item) and not valid_reverse_iso_date(item) and not valid_uk_date(item):
        logger.warning("Bad date : {0}".format(item))

        return False
    return True


def get_datetime_value(item):
    """
    Check if a date item is non-null and parses as ISO (YYYY-MM-DD), reverse-ISO (DD-MM-YYYY),
    or UK format (DD/MM/YYYY).
    Returns a datetime object if successful, None otherwise.
    """
    date_formats = [
        "%Y-%m-%d",  # ISO format (YYYY-MM-DD)
        "%d-%m-%Y",  # Reverse ISO format (DD-MM-YYYY)
        "%d/%m/%Y",  # UK old-style format (DD/MM/YYYY)
    ]
    
    for date_format in date_formats:
        try:
            return datetime.datetime.strptime(item, date_format)
        except ValueError:
            continue
    
    # If we get here, none of the formats worked
    return None


def parse_date(item):
    """
    Crude hand-coded check on date format
    """
    datedata = item.split("-")
    if len(datedata) != 3:
        datedata = item.split("/")
    if len(datedata) != 3:
        return None
    if len(datedata[2]) == 4:
        return(f"{datedata[2]}-{datedata[1]}-{datedata[0]}".format(datedata[2], datedata[1], datedata[0]))
    return "-".join(datedata[:3])

def valid_iso_date(item):
    """
    Check if a date item is non null and parses as ISO (YYYY-MM-DD)
    """
    try:
        datetime.datetime.strptime(item, "%Y-%m-%d")
    except ValueError:
        return False

    return True


def valid_reverse_iso_date(item):
    """
    Check if a date item is non null and parses as reverse ISO (DD-MM-YYYY)
    """
    try:
        datetime.datetime.strptime(item, "%d-%m-%Y")
    except ValueError:
        return False

    return True


def valid_uk_date(item):
    """
    Check if a date item is non null and parses as UK format (DD/MM/YYYY)
    """
    try:
        datetime.datetime.strptime(item, "%d/%m/%Y")
    except ValueError:
        return False

    return True


# End of date code

def load_last_used_ids(last_used_ids_file: Path, last_used_ids):
    fh = open(last_used_ids_file, mode="r", encoding="utf-8-sig")
    csvr = csv.reader(fh, delimiter="\t")

    for last_ids_data in csvr:
        last_used_ids[last_ids_data[0]] = int(last_ids_data[1]) + 1

    fh.close()
    return last_used_ids


def load_saved_person_ids(person_file: Path):
    fh = open(person_file, mode="r", encoding="utf-8-sig")
    csvr = csv.reader(fh, delimiter="\t")
    last_int = 1
    person_ids = {}

    next(csvr)
    for persondata in csvr:
        person_ids[persondata[0]] = persondata[1]
        last_int += 1

    fh.close()
    return person_ids, last_int

def load_person_ids(saved_person_id_file, person_file, mappingrules, use_input_person_ids, delim=","):
    person_ids, person_number = get_person_lookup(saved_person_id_file)

    fh = open(person_file, mode="r", encoding="utf-8-sig")
    csvr = csv.reader(fh, delimiter=delim)
    person_columns = {}
    person_col_in_hdr_number = 0
    reject_count = 0

    personhdr = next(csvr)
    logger.info(personhdr)

    # Make a dictionary of column names vs their positions
    for col in personhdr:
        person_columns[col] = person_col_in_hdr_number
        person_col_in_hdr_number += 1

    ## check the mapping rules for person to find where to get the person data) i.e., which column in the person file contains dob, sex
    birth_datetime_source, person_id_source = mappingrules.get_person_source_field_info(
        "person"
    )
    logger.info(
        "Load Person Data {0}, {1}".format(birth_datetime_source, person_id_source)
    )
    
    ## get the column index of the PersonID from the input file
    person_col = person_columns[person_id_source]

    for persondata in csvr:
        if not valid_value(persondata[person_columns[person_id_source]]): #just checking that the id is not an empty string
            reject_count += 1
            continue
        if not valid_date_value(persondata[person_columns[birth_datetime_source]]):
            reject_count += 1
            continue
        if persondata[person_col] not in person_ids: #if not already in person_ids dict, add it
            if use_input_person_ids == "N":
                person_ids[persondata[person_col]] = str(person_number) #create a new integer person_id
                person_number += 1
            else:
                person_ids[persondata[person_col]] = str(persondata[person_col]) #use existing person_id
    fh.close()

    return person_ids, reject_count

@click.group(help="Commands for using python configurations to run the ETL transformation.")
def py():
    pass


def check_dir_isvalid(directory: Path | tuple[Path, ...]) -> None:
    ## check output dir is valid
    if type(directory) is tuple:
        directory = directory[0]

    assert isinstance(directory, Path)

    if not directory.is_dir():
        logger.warning("Not a directory, dir {0}".format(directory))
        sys.exit(1)


def set_saved_person_id_file(
    saved_person_id_file: Path | None, output_dir: Path
) -> Path:
    """check if there is a saved person id file set in options - if not, check if the file exists and remove it"""

    if saved_person_id_file is None:
        saved_person_id_file = output_dir / "person_ids.tsv"
        if saved_person_id_file.exists():
            assert not saved_person_id_file.is_dir()
            saved_person_id_file.unlink()
    else:
        assert not saved_person_id_file.is_dir()
    return saved_person_id_file

def check_files_in_rules_exist(rules_input_files: list[str], existing_input_files: list[str]) -> None:
    for infile in existing_input_files:
        if infile not in rules_input_files:
            msg = (
                "WARNING: no mapping rules found for existing input file - {0}".format(
                    infile
                )
            )
            logger.warning(msg)
    for infile in rules_input_files:
        if infile not in existing_input_files:
            msg = "WARNING: no data for mapped input file - {0}".format(infile)
            logger.warning(msg)

def open_file(file_path: Path) -> tuple[IO[str], Iterator[list[str]]] | None:
    """opens a file and does something related to CSVs"""
    try:
        fh = open(file_path, mode="r", encoding="utf-8-sig")
        csvr = csv.reader(fh)
        return fh, csvr
    except IOError as e:
        logger.exception("Unable to open: {0}".format(file_path))
        logger.exception("I/O error({0}): {1}".format(e.errno, e.strerror))
        return None


def set_omop_filenames(
    omop_ddl_file: Path, omop_config_file: Path, omop_version: str
) -> tuple[Path, Path]:
    if (
        (omop_ddl_file is None)
        and (omop_config_file is None)
        and (omop_version is not None)
    ):
        omop_config_file = (
            importlib.resources.files("carrottransform") / "config/omop.json"
        )
        omop_ddl_file_name = "OMOPCDM_postgresql_" + omop_version + "_ddl.sql"
        omop_ddl_file = (
            importlib.resources.files("carrottransform") / "config" / omop_ddl_file_name
        )
    return omop_config_file, omop_ddl_file


def get_person_lookup(saved_person_id_file: str) -> tuple[dict[str, str], int]:
    # Saved-person-file existence test, reload if found, return last used integer
    if os.path.isfile(saved_person_id_file):
        person_lookup, last_used_integer = load_saved_person_ids(saved_person_id_file)
    else:
        person_lookup = {}
        last_used_integer = 1
    return person_lookup, last_used_integer

run.add_command(mapstream,"mapstream")


