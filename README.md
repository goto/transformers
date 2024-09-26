# Transformers

[![test workflow](https://github.com/goto/transformers/actions/workflows/test.yml/badge.svg)](test)
[![build workflow](https://github.com/goto/transformers/actions/workflows/build.yml/badge.svg)](build)

Optimus's transformation plugins are implementations of Task and Hook interfaces that allows
execution of arbitrary jobs in optimus.

# Capabilities

- Transform data by BQ SQL syntax and store the transformed data to certain BQ table
- Execute the transformation process with a certain GCP project
- Support various load method, eg. APPEND, REPLACE, MERGE
- Support Bigquery DML Merge statement to handle spillover case
- Support transformation for partitioned tables such as partition by ingestion time (default) and partition by column
- Dry run support

# Use Cases

Base configurations:
```yaml
# ./job.yaml
...
task:
  name: bq2bq
  config:
    LOAD_METHOD: REPLACE
    SQL_TYPE: STANDARD
    PROJECT: project
    DATASET: dataset
    TABLE: destination
    BQ_SERVICE_ACCOUNT: bq_secret_here
  ...
...
```

```sql
-- ./assets/query.sql
select field1, field2 from `project.dataset.source`
```

## Basic transforming the data and store it to the destination BQ table

Use the base configuration above to extract the data from `project.dataset.source` table. The query written on `./assets/query.sql` is used for selecting the records to be loaded to `project.dataset.destination` table (it's configurable through `PROJECT`, `DATASET`, and `TABLE`). The schema of destination table should match with the schema of the record result of that query. `BQ_SERVICE_ACCOUNT` is mandatory credentials to access the BQ api to execute the query.

## Load the queried records to destination BQ table by appending / replace / merge

How the query result load to destination table is depend on `LOAD_METHOD` configuration. [more about load method](https://github.com/goto/transformers/tree/main/task/bq2bq)

## Extracting data through configurable BQ EXECUTION_PROJECT

`EXECUTION_PROJECT` is an additional configuration for the job to execute the query through non-default project. It's useful for customing the allocation of BQ slots. For example when the job requires a lot of resources, it's better to delegate this execution to another dedicated project.
