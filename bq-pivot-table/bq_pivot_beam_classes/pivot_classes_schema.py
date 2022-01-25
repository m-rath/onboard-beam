

import datetime
import apache_beam as beam
from bq_pivot_beam_functions.pivot_functions import order_fields

class PivotSchema(beam.PTransform):

    def __init__(self, args):

        self.key_fds = args.key_fields
        self.piv_fds = args.pivot_fields
        self.val_fds = args.value_fields

    def expand(self, pcollection):

        pivoted_schema_str = (
            pcollection
            | beam.ParDo(
                FieldSchemas(), self.key_fds, self.piv_fds, self.val_fds)
            | beam.Distinct() 
            | beam.transforms.combiners.ToList()
            | beam.Map(order_fields, self.key_fds)
            | beam.Map(lambda x: ",".join(x))
            )   
        return pivoted_schema_str


class FieldSchemas(beam.DoFn):
    
    def __init__(self):

        self.map = {
            str: "STRING",
            int: "INTEGER",
            float: "FLOAT",
            bytes: "BYTES",
            bool: "BOOLEAN",
            datetime.date: "DATE",
            datetime.time: "TIME",
            datetime.datetime: "DATETIME",
            datetime.datetime.timestamp: "TIMESTAMP",
            list: "STRUCT"
            }
    
    def process(self, elem, key_fds, piv_fds, val_fds):
        
        # key-field schemas remain unchanged
        for key_fd in key_fds:
            v = elem[key_fd]
            if v:
                bq_type = self.map[type(v)]
                yield ":".join([key_fd, bq_type])

        # new schemas may differ from row to row
        for piv_fd in piv_fds:
            for val_fd in val_fds:
                col = '_'.join([str(elem[piv_fd]), val_fd]).replace(' ', '_')
                v = elem[val_fd]
                if v:
                    bq_type = self.map[type(v)]
                    yield ":".join([col, bq_type])
