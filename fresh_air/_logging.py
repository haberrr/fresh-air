from typing import Optional
import logging

import prefect
import prefect.exceptions


def get_logger(name: Optional[str] = None):
    """Return Prefect or Python logger depending on the context.

     When called from within Prefect flow or task will return Prefect logger.
     Otherwise, standard Python logger, with an optional name."""
    try:
        return prefect.get_run_logger()
    except prefect.exceptions.MissingContextError:
        return logging.getLogger(name)
