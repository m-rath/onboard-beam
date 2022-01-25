
from typing import List, Dict, NamedTuple
import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.bigquery_tools import get_table_schema_from_string


PROJECT_ID = 'practice-springml'


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

# unnecessary, because named tuple takes care of it
def order_fields(fd_schemas: List[str], key_fds: List[str]) -> List[str]:
    sorted_field_schemas = sorted(
        fd_schemas, key=lambda x: x.split(":")[0] in key_fds, reverse=True)
    return sorted_field_schemas

def fill_fields(elem: Dict, NTC: NamedTuple) -> Dict:
    for field in NTC._fields:
        if field not in elem:
            elem[field] = ""
    return elem

def schema_fn(destination, schema_str):
    """ structured to Beam's liking """
    return get_table_schema_from_string(schema_str)

# NTC = NamedTuple('NTC', [('title', str), ('count', int)]) # could I make this from dynamic schema_str?
# NTC = NamedTuple('NTC', title=str, count=int) # could I make this from dynamic schema_str?
# with beam.Pipeline(InteractiveRunner()) as p:
#   input = (
#       p 
#       | beam.Create([
#                      {'title': 'yoohoo', 'count': 43},
#                     #  {'title': 'yoowho', 'count': ""}])
#                      {'title': 'yoowho'}])
#       | beam.Map(fill_fields, NTC)
#       | beam.Map(lambda x: NTC(**x)).with_output_types(NTC)
#   )



# sch = 'title:STRING,count:INTEGER'
map = {"STRING":str, "INTEGER":int}

def register_schema(schema_str):

    f_lists = [f.split(":") for f in schema_str.split(',')]
    f_tuples = [((f[0], map[f[1]])) for f in f_lists]
    
    PivotedRecord = NamedTuple('PivotedRecord', f_tuples)
    
    return PivotedRecord
# PivotedRecord = register_schema(sch)

# class PivotedRecordCoder(beam.coders.Coder):
#   def encode(self, prc):
#     # return ('%s:%s' % (prc.title, prc.count)).encode('utf-8')
#     return "::".join([str(prc.__getattribute__(x)) for x in prc._fields])
#     # return prc.__str__()
#   def decode(self, s):
#     return PRC(*s.decode('utf-8').split('::'))
#   def is_deterministic(self):
#     return True
# beam.coders.registry.register_coder(PivotedRecord, PivotedRecordCoder)



# | beam.Map(fill_fields, PivotedRecord)
# | beam.Map(lambda x: PivotedRecord(**x))#.with_output_types(PivotedRecord)
