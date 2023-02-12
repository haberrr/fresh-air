from datetime import datetime
from typing import Iterable, Dict, Any

from prefect.context import get_run_context, TaskRunContext, FlowRunContext

from fresh_air.data.storage.base import SchemaField

meta_fields = [
    SchemaField(
        name='_etl_timestamp',
        field_type='timestamp',
        description='Technical field, timestamp of the ETL job.',
    ),
    # Too much data usage, have to drop it for now.
    # SchemaField(
    #     name='_etl_prefect_flow_run_id',
    #     field_type='string',
    #     description='Technical field, UUID of the Prefect flow run.',
    # ),
    # SchemaField(
    #     name='_etl_prefect_task_run_id',
    #     field_type='string',
    #     description='Technical field, UUID of the Prefect task run.',
    # ),
]


def add_meta(records: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    """Add meta-data to each record."""
    ts = datetime.now().timestamp()
    flow_run_id = None
    task_run_id = None

    try:
        context = get_run_context()
        if isinstance(context, FlowRunContext):
            flow_run_id = str(context.flow_run.flow_id)
        elif isinstance(context, TaskRunContext):
            flow_run_id = str(context.task_run.flow_run_id)
            task_run_id = str(context.task_run.id)
    except RuntimeError:
        pass

    for record in records:
        yield {
            **record,
            '_etl_timestamp': ts,
            # '_etl_prefect_flow_run_id': flow_run_id,
            # '_etl_prefect_task_run_id': task_run_id,
        }
