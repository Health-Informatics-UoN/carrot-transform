"""
Entry point for the v2 processing system
"""

import time
from pathlib import Path
from typing import Optional

from carrottransform.tools import outputs, sources
from carrottransform.tools.args import PathArg
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.orchestrator import V2ProcessingOrchestrator

logger = logger_setup()


def process_common_logic(
    rules_file: Path,
    output: outputs.OutputTarget,
    write_mode: str,
    omop_ddl_file: Path,
    omop_version: Optional[str],
    person: str,
    inputs: sources.SourceObject,
):
    """Common processing logic for both modes"""

    # NOTE; a lot of this is now handled by the run.py script

    start_time = time.time()

    # this used to be a parameter; it's hard coded now but otherwise unchanged
    omop_config_file: Path = PathArg.convert("@carrot/config/config.json", None, None)

    try:
        # Create orchestrator and execute processing (pass explicit kwargs to satisfy typing)
        orchestrator = V2ProcessingOrchestrator(
            rules_file=rules_file,
            output=output,
            inputs=inputs,
            person=person,
            omop_ddl_file=omop_ddl_file,
            omop_config_file=omop_config_file,
            write_mode=write_mode,
        )

        logger.info(
            f"Loaded v2 mapping rules from: {rules_file} in {time.time() - start_time:.5f} secs"
        )

        result = orchestrator.execute_processing()

        if result.success:
            logger.info(
                f"V2 processing completed successfully in {time.time() - start_time:.5f} secs"
            )
        else:
            logger.error(f"V2 processing failed: {result.error_message}")

    except Exception as e:
        logger.error(f"V2 processing failed with error: {str(e)}")
        raise
