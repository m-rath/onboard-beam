
import os
# from dotenv import load_dotenv
from typing import List, Dict
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.bigquery_tools import get_table_schema_from_string

# load_dotenv()
PROJECT_ID = os.getenv('PROJECT_ID')

def validate_table(args):
    bq_wrap = BigQueryWrapper()
    dataset, table_name = args.input_table_spec.split(r'.')
    try:
        bq_table = bq_wrap.get_table(PROJECT_ID, dataset, table_name)
    except:
        print(f"Table '{args.input_table_spec}' not found \n" \
            f"Please double-check project '{PROJECT_ID}' for table")
        return
    return bq_table


def validate_fields(bq_table, args):
    fd_names = [fd.name for fd in bq_table.schema.fields]
    target_cols = [*args.key_fields, *args.pivot_fields, *args.value_fields]
    if all(f in fd_names for f in target_cols):
        query_cols = ",".join([col for col in target_cols])
        query = rf'SELECT {query_cols} FROM {args.input_table_spec};'
    else:
        print("Not all user-provided field names appear in specified table.")
        return
    return query

# unnecessary if NamedTuple handles it downstream
def order_fields(fd_schemas: List[str], key_fds: List[str]) -> List[str]:
    sorted_field_schemas = sorted(
        fd_schemas, key=lambda x: x.split(":")[0] in key_fds, reverse=True)
    return sorted_field_schemas

def fill_fields(elem, new_schema_list):
    e = {}
    for field_schema in new_schema_list:
        field_name = field_schema.split(":")[0]
        if field_name in elem:          #MPTypeError: unhashable type: 'list' [while running 'PivotRecords/Map(fill_fields)/Map(fill_fields)']
            e[field_name] = elem[field_name]
        else:
            e[field_name] = ""
    return e
    # field_name = field_schema.split(":")[0]
# AttributeError: 'list' object has no attribute 'split' [while running 'PivotRecords/Map(fill_fields)/Map(fill_fields)'] 
def schema_fn(destination, schema_str):
    """ structured to Beam's liking """
    return get_table_schema_from_string(schema_str)
