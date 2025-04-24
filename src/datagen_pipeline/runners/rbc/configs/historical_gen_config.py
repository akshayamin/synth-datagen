## Source
source_reference_tbl = "aa_catalog.org1.silver_testtable_15" # Table to pull table schema from. "custom_source_schema" overrides this. Can be None

## Target
target_table_name = "aa_catalog.org1.gentable_701" # table to write the delta data
target_data_path = "s3://databricks-akshayamin/data/gentable_701/" # table path to write the delta data in case of external locations being used

## Write Details
nrows = 40000 # Number of rows to be generated
n_gen_partitions = 4 # Number of spark partitions.
write_mode="overwrite" # options ("overwrite"|"append")
hive_partitions_limit_override_flag=False # options (False|True)
partition_col_list = ["category"] # List of partition columns. This will override "schema_source_reference" table partition columns

## Run Mode
run_mode = "dev" # value "debug" if you don't want to write to table. Options ("debug"|<any thing else>)


def get_config():
    config = {
        "target_table_name": target_table_name,
        "target_data_path": target_data_path,
        "source_reference_tbl": source_reference_tbl,
        "nrows": nrows,
        "n_gen_partitions": n_gen_partitions,
        "write_mode": write_mode,
        "hive_partitions_limit_override_flag": hive_partitions_limit_override_flag,
        "run_mode": run_mode,
        "partition_col_list": partition_col_list
    }
    return config

