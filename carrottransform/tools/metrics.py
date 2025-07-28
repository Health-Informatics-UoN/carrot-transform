from dataclasses import dataclass, field
from typing import Dict, List

from carrottransform.tools.logger import logger_setup

logger = logger_setup()


@dataclass
class DataKey:
    source: str
    fieldname: str
    tablename: str
    concept_id: str
    additional: str

    def __str__(self) -> str:
        """
        The original implementation used strings as keys, then split by `~`.
        This is here in case that representation is needed somewhere
        """
        return f"{self.source}~{self.fieldname}~{self.tablename}~{self.concept_id}~{self.additional}"

    def __hash__(self) -> int:
        """
        The DataKey is used as a key for a dictionary of key counts
        """
        return hash(
            (
                self.source,
                self.fieldname,
                self.tablename,
                self.concept_id,
                self.additional,
            )
        )


@dataclass
class CountData:
    counts: Dict[str, int] = field(default_factory=dict)

    def increment(self, count_type: str):
        if count_type not in self.counts:
            self.counts[count_type] = 0
        self.counts[count_type] += 1

    def get_count(self, count_type: str, default: int = 0):
        return self.counts.get(count_type, default)


@dataclass
class MapstreamSummaryRow:
    """Represents a single row in the mapstream summary"""

    dataset_name: str
    source: str
    fieldname: str
    tablename: str
    concept_id: str
    additional: str
    input_count: int = 0
    invalid_person_ids: int = 0
    invalid_date_fields: int = 0
    invalid_source_fields: int = 0
    output_count: int = 0

    def to_tsv_row(self) -> str:
        """Convert the row to a tab-separated string"""
        row_list = [
            str(col)
            for col in [
                self.dataset_name,
                self.source,
                self.fieldname,
                self.tablename,
                self.concept_id,
                self.additional,
                self.input_count,
                self.invalid_person_ids,
                self.invalid_date_fields,
                self.invalid_source_fields,
                self.output_count,
            ]
        ]
        # If python gets updated, you can move the row_str expression into the f-string
        row_str = "\t".join(row_list)
        return f"{row_str}\n"

    @classmethod
    def get_header(cls) -> str:
        """Return the TSV header row"""
        header = [
            "dsname",
            "source",
            "source_field",
            "target",
            "concept_id",
            "additional",
            "incount",
            "invalid_persid",
            "invalid_date",
            "invalid_source",
            "outcount",
        ]
        header_str = "\t".join(header)
        return f"{header_str}\n"


class Metrics:
    """
    Capture metrics for output to a summary tsv file, record counts at multiple levels
    The main principle is to increment counts associated with datakeys (dkey) at different levels
    """

    def __init__(self, dataset_name, log_threshold=0):
        """
        self.datasummary holds all the saved counts
        """
        self.datasummary = {}
        self.allcounts = {}
        self.dataset_name = dataset_name
        self.log_threshold = log_threshold

    def get_new_mapstream_counts(self):
        """
        return a new, initialised,  count structure
        """
        counts = {}
        counts["input_count"] = 0
        counts["invalid_persids"] = 0
        counts["invalid_dates"] = 0
        counts["invalid_source_fields"] = 0
        counts["output_count"] = 0

        return counts

    def add_data(self, desttablename, increment):
        """
        add_data(self, destination table, data increment)
        Apply the contents of a data increment to the stored self.datasummary
        """
        name = increment["name"]
        for datakey, dataitem in increment.items():
            if datakey == "valid_person_id":
                dkey = "NA" + "." + desttablename + "." + name + "." + datakey
                self.add_counts_to_summary(dkey, dataitem)
            elif datakey == "person_id":
                dkey = "NA" + "." + desttablename + "." + name + "." + datakey
                self.add_counts_to_summary(dkey, dataitem)
            elif datakey == "required_fields":
                for fieldname in dataitem:
                    prfx = "NA"
                    if "source_files" in increment:
                        if fieldname in increment["source_files"]:
                            prfx = self.get_prefix(
                                increment["source_files"][fieldname]["table"]
                            )
                            dkey = (
                                prfx
                                + "."
                                + desttablename
                                + "."
                                + name
                                + "."
                                + fieldname
                            )
                            self.add_counts_to_summary(dkey, dataitem[fieldname])

    def get_prefix(self, fname):
        return fname.split(".")[0]

    def add_counts_to_summary(self, dkey, count_block):
        if dkey not in self.datasummary:
            self.datasummary[dkey] = {}
        for counttype in count_block:
            if counttype not in self.datasummary[dkey]:
                self.datasummary[dkey][counttype] = 0
            self.datasummary[dkey][counttype] += int(count_block[counttype])

    def increment_key_count(
        self, source, fieldname, tablename, concept_id, additional, count_type
    ):
        dkey = DataKey(source, fieldname, tablename, concept_id, additional)

        if dkey not in self.datasummary:
            self.datasummary[dkey] = CountData()

        self.datasummary[dkey].increment(count_type)

    def increment_with_datacol(
        self,
        source_path: str,
        target_file: str,
        datacol: str,
        out_record: List[str],
    ) -> None:
        # Are the parameters for DataKeys hierarchical?
        # If so, a nested structure where a Source contains n Fields etc. and each has a method to sum its children would be better
        # But I don't know if that's the desired behaviour

        # A lot of these increment the same thing, so I have defined `increment_this`
        def increment_this(
            fieldname: str,
            concept_id: str,
            additional="",
        ) -> None:
            self.increment_key_count(
                source=source_path,
                fieldname=fieldname,
                tablename=target_file,
                concept_id=concept_id,
                additional=additional,
                count_type="output_count",
            )

        self.increment_key_count(
            source=source_path,
            fieldname="all",
            tablename="all",
            concept_id="all",
            additional="",
            count_type="output_count",
        )

        self.increment_key_count(
            source="all",
            fieldname="all",
            tablename=target_file,
            concept_id="all",
            additional="",
            count_type="output_count",
        )
        increment_this(fieldname="all", concept_id="all")

        if target_file == "person":
            increment_this(fieldname="all", concept_id=out_record[1])
            increment_this(
                fieldname="all", concept_id=out_record[1], additional=out_record[2]
            )
        else:
            increment_this(fieldname=datacol, concept_id=out_record[2])
            increment_this(fieldname="all", concept_id=out_record[2])
            self.increment_key_count(
                source="all",
                fieldname="all",
                tablename=target_file,
                concept_id=out_record[2],
                additional="",
                count_type="output_count",
            )
            self.increment_key_count(
                source="all",
                fieldname="all",
                tablename="all",
                concept_id=out_record[2],
                additional="",
                count_type="output_count",
            )

    def get_summary(self):
        summary_str = "source\ttablename\tname\tcolumn name\tbefore\tafter content check\tpct reject content check\tafter date format check\tpct reject date format\n"

        for dkey in self.datasummary:
            logger.debug(dkey)
            source, tablename, name, colname = dkey.split(".")
            before_count = int(self.datasummary[dkey]["before"])
            after_count = int(self.datasummary[dkey]["after"])
            after_pct = (float)(before_count - after_count) * 100 / before_count
            summary_str += (
                source
                + "\t"
                + tablename
                + "\t"
                + name
                + "\t"
                + colname
                + "\t"
                + str(before_count)
                + "\t"
                + str(after_count)
                + "\t"
                + "{0:.3f}".format(after_pct)
                + "\t"
            )
            if "after_formatting" in self.datasummary[dkey]:
                after_format_count = int(self.datasummary[dkey]["after_formatting"])
                after_format_pct = (
                    (float)(after_count - after_format_count) * 100 / after_count
                )
                summary_str += (
                    str(after_format_count)
                    + "\t"
                    + "{0:.3f}".format(after_format_pct)
                    + "\n"
                )
            else:
                summary_str += "NA\tNA\n"

        return summary_str

    def get_data_summary(self):
        return self.datasummary

    def get_mapstream_summary_rows(self) -> List[MapstreamSummaryRow]:
        """
        Creates a list of MapstreamSummaryRow from the datasummary
        """
        rows = []

        for d_key in sorted(self.datasummary.keys(), key=str):
            source = self.get_prefix(d_key.source)
            count_data = self.datasummary[d_key]

            row = MapstreamSummaryRow(
                dataset_name=self.dataset_name,
                source=source,
                fieldname=d_key.fieldname,
                tablename=d_key.tablename,
                concept_id=d_key.concept_id,
                additional=d_key.additional,
                input_count=count_data.get_count("input_count"),
                invalid_person_ids=count_data.get_count("invalid_person_ids"),
                invalid_date_fields=count_data.get_count("invalid_date_fields"),
                invalid_source_fields=count_data.get_count("invalid_source_fields"),
                output_count=count_data.get_count("output_count"),
            )

            if row.output_count >= self.log_threshold:
                rows.append(row)
        return rows

    def get_mapstream_summary(self) -> str:
        """
        Makes a TSV string of the mapstream summary
        """
        summary_rows = self.get_mapstream_summary_rows()
        result = MapstreamSummaryRow.get_header()

        for row in summary_rows:
            result += row.to_tsv_row()

        return result

    def get_mapstream_summary_dict(self) -> Dict:
        """
        Makes a dict of the mapstream summary
        """
        rows = self.get_mapstream_summary_rows()
        return {
            "dataset": self.dataset_name,
            "threshold": self.log_threshold,
            "rows": [vars(row) for row in rows],
        }
