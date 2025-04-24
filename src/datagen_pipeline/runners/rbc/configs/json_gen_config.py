## Source
json_source_table_name = "aa_catalog.org1.silver_57" # source table to get data to be converted into JSON

## Target
json_target_path = "s3://databricks-akshayamin/data/json/s56/" # Bronze path to write the JSON files

## Mappings
json_target_mapper_str = {
    "header": ["pid_str", {"transaction": ["email", "category"]}],
    "paycore": ["ld_multiplier_1"],
} # json schema for each row to be converted into
additional_root_fields_to_extract = ["station_id", "test_id"] # Root fields for fields to be extracted from silver table

## Write Details
json_target_write_method="append" # write modes ("overwrite"|"append")
jsondata_file_format = "json" # Fileformat Options: (json | avro | parquet)
jsondata_compression = "None" # Compression format for the files to be written Options: (gzip | None)

## Run Mode
run_mode = "dev" # value "debug" if you don't want to write to table. Options ("debug"|<any thing else>)

def get_config():
    # Define your configuration directly in a dictionary.
    config = {
        "json_target_mapper_str": json_target_mapper_str,
        "json_target_path": json_target_path,
        "additional_root_fields_to_extract": additional_root_fields_to_extract,
        "json_source_table_name": json_source_table_name,
        "json_target_write_method": json_target_write_method,
        "jsondata_file_format": jsondata_file_format,
        "jsondata_compression": jsondata_compression,
        "run_mode": run_mode
    }
    return config
