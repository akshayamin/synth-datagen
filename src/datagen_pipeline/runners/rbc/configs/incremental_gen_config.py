import pyspark.sql.functions as F

## Source
target_table_name= "aa_catalog.org1.gentable_701" # This is the target table for DLT pipeline. table to read data to generate updates. 
source_reference_tbl = "aa_catalog.org1.silver_testtable_15"  # Table to pull table schema from. "initial_custom_schema" overrides this

## Target
incremental_perist_table = "aa_catalog.org1.gentable_703_inc" # target table to write the incremental generated records
incremental_perist_data_path = None # Table path to write the delta data in case of external locations being used
persist_incrementals=True # whether to persist incremental data in delta format before writing to json bronze

## Write Details
nrows = 10000 # Number of rows to be generated
n_gen_partitions = 10 # Number of spark partitions.
hive_partitions_limit_override_flag=False # options (False|True)
partition_col_list = ["category"] # List of partition columns. This will override "schema_source_reference" table partition columns

## Run Mode
run_mode = "dev" # value "debug" if you don't want to write to table

## Incremental insert properties
insert_filter=None # Filter on target_table_name to get max primary key. "None" will default to "F.lit(True)"

## Incremental Update properties
incremental_update_ratio=0.2 # Ratio of incremental records that are updates.
updatefields = ["email", "category", "ld_multiplier_1", "name", "age", "ld_multiplier_2"] # specify the fields to be updated as part of the incremental updates batch of records

# update_filter = F.expr("modified_ts BETWEEN '2024-02-02 00:00:00' AND '2024-04-15 00:00:00' AND sequence_date BETWEEN '2021-10-09' AND '2021-10-11'") # filter criteria to apply to "source_data_table_name" when pulling records to generate updates. update_filter can be any "expression" that goes as an argument to the df.filter(expression)
update_filter=F.lit(True)

## Sequence Fields properties
sequencefields = {
# "modified_ts": {"insertstart": '2024-06-01 00:00:00', "insertend": '2024-06-30 00:00:00', "updatestart": '2024-06-01 00:00:00',"updateend": '2024-06-30 00:00:00'},
"sequence_field": {"insertstart": 2000, "insertend": 3000, "updatestart": 1000,"updateend": 2000},
"sequence_date": {"insertstart": '2025-06-01', "insertend": '2025-08-01', "updatestart": '2026-06-01',"updateend": '2026-08-01'},
} # sequence fields are used to APPLY CHANGES during incremental load
# Typically sequencefields are int or timestamp. Specify start/end values for sequencefields for inserts & updates set of records

update_field_stats_table="aa_catalog.org1.datagen_update_field_stats_v8" # table to capture distinct field values for generated updated values for the field

def get_config():
    config = {
        "target_table_name": target_table_name,
        "incremental_perist_table": incremental_perist_table,
        "incremental_perist_data_path": incremental_perist_data_path,
        "source_reference_tbl": source_reference_tbl,
        "nrows": nrows,
        "n_gen_partitions": n_gen_partitions,
        "hive_partitions_limit_override_flag": hive_partitions_limit_override_flag,
        "run_mode": run_mode,
        "partition_col_list": partition_col_list,
        "incremental_update_ratio": incremental_update_ratio,
        "updatefields": updatefields,
        "update_filter": update_filter,
        "sequencefields": sequencefields,
        "persist_incrementals": persist_incrementals,
        "insert_filter": insert_filter,
        "update_field_stats_table": update_field_stats_table
    }
    return config