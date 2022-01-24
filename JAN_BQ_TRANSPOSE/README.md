goals of Beam PivotTable re-do:
- generally, improve modularity
  - learn to run dataflow job from pipeline that imports local modules
- refactor for easier read and re-use
  - extract schema without BigQueryWrapper, for wider application
  - finish with schema'd pcollection, for wider application
  - allow for multiple key_fields of any type
  - allow for multiple value_fields
  - improve deconstruct(groupby)...reconstruct routine, for aggregation with row dictionaries
    - <https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/group_with_coder.py>
    - <https://beam.apache.org/documentation/sdks/python-type-safety/#use-of-type-hints-in-coders>
    - cast to int or float might be necessary on reconstruct, after groupby and combine
- demonstrate usefulness with a real dataset -- names, by gender and region
  - first replace states with regions
  - then pivot with key_fields = ['name', 'gender'], pivot_fields = ['region'], value_fields = ['number'] 
  - then pivot resulting table again with key_fields = ['name'], pivot_fields = ['gender'], value_fields = ['...']
  - with this more general pipeline, repeat pivot can happen without paying to write intermediate table to Big Query

tasks that remain:
- attach schema to pcollection, even before WriteToBigQuery
  - beam methods can convert schema_str to table_schema; how is that attached to each row?
  - or change rows from dicts to lists of tuples, then register NamedTuple to schema pcollection?