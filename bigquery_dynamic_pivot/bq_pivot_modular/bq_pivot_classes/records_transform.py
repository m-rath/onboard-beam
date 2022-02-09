
from email.generator import Generator
from typing import Tuple, List, Dict, Iterable
import apache_beam as beam
from apache_beam.pvalue import AsSingleton, AsList, AsIter

from bq_pivot_functions.pivot_functions import fill_fields


class PivotRecords(beam.PTransform):

    def __init__(self, args, new_schema_list):
        self.key_fds = args.key_fields
        self.piv_fds = args.pivot_fields
        self.val_fds = args.value_fields
        self.new_schema_list = new_schema_list

    def expand(self, pcollection):

        pivoted_records = (
            pcollection 
            | beam.ParDo(PivotRow(), self.key_fds, self.piv_fds, self.val_fds)
            | beam.GroupBy(
                lambda x: "__".join([str(x[kfd]) for kfd in self.key_fds]))
            | beam.CombinePerKey(CollapsePivoted(), self.key_fds)
            | beam.ParDo(ReconstrElem(), self.key_fds)
            )

        return pivoted_records


class PivotRow(beam.DoFn):

    def process(self, elem, key_fds, piv_fds, val_fds):
        
        # start with the key_fields, unchanged
        pivoted_row = {k:v for k,v in elem.items() if k in key_fds}
        
        # then update with new fields
        for piv_fd in piv_fds:
            for val_fd in val_fds:
                col = "_".join([str(elem[piv_fd]), val_fd]).replace(' ', '_')
                pivoted_row.update({col: elem[val_fd]})

        yield pivoted_row


class CollapsePivoted(beam.CombineFn):
    """ 
    accepts as side input a list of 1 or more keyfields, of any type;
    sums all other fields, assumed numeric;
    
    TO-Do: accommodate string value_fields, 
    but throw error if more than 1 distinct value, or extend into list
    """
    def create_accumulator(self, keyfields):
        return {}

    def add_input(self, accumulator, input, keyfields):

        for dct in input:
            for k,v in dct.items():
                if k not in keyfields and type(v) != str:
                    if v is None:
                        v = 0
                    if k not in accumulator:
                        accumulator[k] = v
                    else:
                        accumulator[k] += v
        return accumulator

    def merge_accumulators(self, accumulators, keyfields):
        merged = {}
        for accum in accumulators:
            for k, v in accum.items():
                if type(v) != str:
                    if v is None:
                        v = 0
                    if k not in merged:
                        merged[k] = v
                    else:
                        merged[k] += v
        return merged

    def extract_output(self, accumulator, keyfields):
        return accumulator

class ReconstrElem(beam.DoFn):
    """
    two-tuple to dict
    Tup[int, dict] -> Dict

    TO-DO: keyfield values cast to string for new column headings
    reconstruct as string, even if int to begin; fix that 
    """
    def process(self, element, keyfields):
        key, val = element
        keylist = key.split("__")
        reconstr_elem = {k:v for k,v in zip(keyfields, keylist)}
        reconstr_elem.update(val)
        yield reconstr_elem
