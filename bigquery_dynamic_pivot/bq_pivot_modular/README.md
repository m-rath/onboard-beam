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
  - register NamedTuple, then lambda x: NT(**x), where x is row Dict?
- edit CombineFn to allow for string value fields<br><br><br>




## baby_names.orig_table
![](.\imgs\orig_table.PNG)
<br><br>

## baby_names.pivoted_region1
![](.\imgs\pivot_on_region_then_sum.PNG)
<br><br>

## baby_names.pivoted_region2
![](.\imgs\then_pivot_again_on_gender.PNG)
<br><br>


## dataflow job execution graphs

dataflow job execution graph, with regional groupings
![](.\imgs\job_execution_graph_with_extras.PNG)
<br><br>

execution graph, collapsed
![](.\imgs\job_execution_graph_collapsed.PNG)
<br><br>

execution graph, detailed
![](.\imgs\job_execution_graph.PNG)
<br><br>