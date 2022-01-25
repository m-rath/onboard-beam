goals of Beam PivotTable re-do:
- generally, improve modularity
- refactor for easier read and re-use
  - extract schema without BigQueryWrapper this time, for wider application
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
  - register NamedTuple, then lambda x: NT(**x), where x is row Dict