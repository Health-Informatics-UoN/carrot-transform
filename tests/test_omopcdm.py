import datetime
import logging
import re
from pathlib import Path

import pytest

import carrottransform.tools.date_helpers as date_helpers
import carrottransform.tools.omopcdm as omopcdm
import tests.click_tools as click_tools
import tests.csvrow as csvrow


@pytest.mark.unit
def test_nofile(caplog, tmp_path):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit):
            cdm = omopcdm.OmopCDM(tmp_path / "foo.sql", tmp_path / "bar.jsn")

        # check the specific log record
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == "ERROR"
        assert caplog.records[0].msg.startswith("OMOP ddl file (")
        assert caplog.records[0].msg.endswith("foo.sql) not found")
