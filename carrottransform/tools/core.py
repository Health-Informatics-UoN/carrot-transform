import carrottransform.tools as tools
from carrottransform.tools.date_helpers import get_datetime_value
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.validation import valid_value

logger = logger_setup()


def get_target_records(
    tgtfilename: str,
    tgtcolmap: dict[str, int],
    rulesmap: dict[str, list[dict[str, list[str]]]],
    srcfield: str,
    srcdata: list[str],
    srccolmap: dict[str, int],
    srcfilename: str,
    omopcdm: OmopCDM,
    metrics: tools.metrics.Metrics,
) -> tuple[bool, list[list[str]], tools.metrics.Metrics]:
    """
    build all target records for a given input field
    """
    build_records = False
    tgtrecords = []
    # Get field definitions from OMOP CDM
    date_col_data = omopcdm.get_omop_datetime_linked_fields(tgtfilename)
    date_component_data = omopcdm.get_omop_date_field_components(tgtfilename)
    notnull_numeric_fields = omopcdm.get_omop_notnull_numeric_fields(tgtfilename)

    # Build keys to look up rules
    srckey = f"{srcfilename}~{srcfield}~{tgtfilename}"

    # Check if source field has a value
    if valid_value(str(srcdata[srccolmap[srcfield]])):
        ## check if either or both of the srckey and summarykey are in the rules
        srcfullkey = (
            srcfilename
            + "~"
            + srcfield
            + "~"
            + str(srcdata[srccolmap[srcfield]])
            + "~"
            + tgtfilename
        )

        dictkeys = []
        # Check if we have rules for either the full key or just the source field
        if tgtfilename == "person":
            build_records = True
            dictkeys.append(srcfilename + "~person")
        elif srcfullkey in rulesmap:
            build_records = True
            dictkeys.append(srcfullkey)
        if srckey in rulesmap:
            build_records = True
            dictkeys.append(srckey)

        if build_records:
            # Process each matching rule
            for dictkey in dictkeys:
                # fixed a crash that seemed to happen when there were only Male mappings
                if dictkey not in rulesmap.keys():
                    continue
                for out_data_elem in rulesmap[dictkey]:
                    valid_data_elem = True
                    ## create empty list to store the data. Populate numerical data elements with 0 instead of empty string.
                    tgtarray = [""] * len(tgtcolmap)
                    # Initialize numeric fields to 0
                    for req_integer in notnull_numeric_fields:
                        tgtarray[tgtcolmap[req_integer]] = "0"

                    # Process each field mapping
                    for infield, outfield_list in out_data_elem.items():
                        if tgtfilename == "person" and isinstance(outfield_list, dict):
                            # Handle term mappings for person records
                            input_value = srcdata[srccolmap[infield]]
                            if str(input_value) in outfield_list:
                                for output_col_data in outfield_list[str(input_value)]:
                                    if "~" in output_col_data:
                                        # Handle mapped values (like gender codes)
                                        outcol, term = output_col_data.split("~")
                                        tgtarray[tgtcolmap[outcol]] = term
                                    else:
                                        # Direct field copy
                                        tgtarray[tgtcolmap[output_col_data]] = srcdata[
                                            srccolmap[infield]
                                        ]
                        else:
                            # Handle direct field copies and non-person records
                            for output_col_data in outfield_list:
                                if "~" in output_col_data:
                                    # Handle mapped values (like gender codes)
                                    outcol, term = output_col_data.split("~")
                                    tgtarray[tgtcolmap[outcol]] = term
                                else:
                                    # Direct field copy
                                    tgtarray[tgtcolmap[output_col_data]] = srcdata[
                                        srccolmap[infield]
                                    ]

                            # get the value. this is out 8061 value that was previously normalised
                            source_date = srcdata[srccolmap[infield]]

                            # Special handling for date fields
                            if output_col_data in date_component_data:
                                # this side of the if/else seems to be fore birthdates which're split up into four fields

                                # parse the date and store it in the old format ... as a way to branch
                                # ... this check might be redudant. the datetime values should be ones that have already been normalised
                                dt = get_datetime_value(source_date.split(" ")[0])
                                if dt is None:
                                    # if (as above) dt isn't going to be None than this branch shouldn't happen
                                    # maybe brithdates can be None?

                                    metrics.increment_key_count(
                                        source=srcfilename,
                                        fieldname=srcfield,
                                        tablename=tgtfilename,
                                        concept_id="all",
                                        additional="",
                                        count_type="invalid_date_fields",
                                    )
                                    valid_data_elem = False
                                else:
                                    year_field = date_component_data[output_col_data][
                                        "year"
                                    ]
                                    month_field = date_component_data[output_col_data][
                                        "month"
                                    ]
                                    day_field = date_component_data[output_col_data][
                                        "day"
                                    ]
                                    tgtarray[tgtcolmap[year_field]] = str(dt.year)
                                    tgtarray[tgtcolmap[month_field]] = str(dt.month)
                                    tgtarray[tgtcolmap[day_field]] = str(dt.day)

                                    tgtarray[tgtcolmap[output_col_data]] = source_date

                            elif (
                                output_col_data in date_col_data
                            ):  # date_col_data for key $K$ is where $only_date(srcdata[K])$ should be copied and is there for all dates
                                # this fork of the if/else seems to be for non-birthdates which're handled differently

                                # copy the full value into this "full value"
                                tgtarray[tgtcolmap[output_col_data]] = source_date

                                # select the first 10 chars which will be YYYY-MM-DD
                                tgtarray[tgtcolmap[date_col_data[output_col_data]]] = (
                                    source_date[:10]
                                )

                    if valid_data_elem:
                        tgtrecords.append(tgtarray)
    else:
        metrics.increment_key_count(
            source=srcfilename,
            fieldname=srcfield,
            tablename=tgtfilename,
            concept_id="all",
            additional="",
            count_type="invalid_source_fields",
        )

    return build_records, tgtrecords, metrics
