import pyspark.sql.functions as F

class CommonConfig:
    def __init__(self,
        spark,
        compute_stats_table=None,
        custom_source_schema=None,
        primary_key=None,
        colspec_overrides=None,
        default_colspecs=None):

        self.spark=spark # spark session variable
        self.custom_source_schema = custom_source_schema # custom schema for generated data in StructType format
        self.primary_key = primary_key # List of primary key(s) for the generated dataset
        self.default_colspecs = default_colspecs # default specifications for different datatypes
        self.colspec_overrides = colspec_overrides  # override specs for specific columns as needed. "derive": True will derive values 
                                                    # from given field from source_reference_tbl or source_reference_df
        self.compute_stats_table=compute_stats_table

class HistoricalConfig():
    def __init__(self,
        target_table_name,
        target_data_path=None,
        load_type="historical",
        source_reference_tbl=None,
        source_reference_df=None,
        nrows=10000,
        n_gen_partitions=200,
        write_mode="append",
        hive_partitions_limit_override_flag=False,
        run_mode="dev",
        partition_col_list=[]):

        self.target_table_name=target_table_name # Target table name to be written.
        self.target_data_path=target_data_path # Write path for target generated data. If target_data_path also passed, then it creates an external table else managed
        self.load_type=load_type # "historical" by default
        self.source_reference_tbl=source_reference_tbl # "if custom_source_schema & source_reference_df is not provided, use this to get schema. 
                                                       # If partition_col_list is not provided, use this table to derive partition information". 
                                                       # if colspec_overrides has a rule with "derive": True and if source_reference_df is None, 
                                                       # use this table to derive the values for the derived columns
        self.source_reference_df=source_reference_df # if custom_schema is not provided, use this dataframe derived in the entry point code to derive customer schema
        self.nrows=nrows # number of rows to be generated
        self.n_gen_partitions=n_gen_partitions # number of spark partitions to be generated
        self.write_mode=write_mode # options ("overwrite"|"append")
        self.hive_partitions_limit_override_flag=hive_partitions_limit_override_flag # options (False|True)
        self.run_mode=run_mode # value "debug" if you don't want to write to table. Options ("debug"|<any thing else>)
        self.partition_col_list = partition_col_list # List of partition columns. This will override "schema_source_reference" table partition columns

class JsonifyConfig():
    def __init__(self,
        json_target_mapper_str, 
        json_target_path, 
        additional_root_fields_to_extract=None, 
        json_source_table_name=None, 
        json_source_df=None, 
        json_target_write_method="append", 
        jsondata_file_format="json", 
        jsondata_compression="None", 
        run_mode = "dev"): 

        self.json_target_mapper_str=json_target_mapper_str # json schema for each row to be converted into
        self.json_target_path=json_target_path # Bronze path to write the JSON files
        self.additional_root_fields_to_extract=additional_root_fields_to_extract # Root fields for fields to be extracted from silver table
        self.json_source_table_name=json_source_table_name # source table to get data to be converted into JSON
        self.json_source_df=json_source_df # source dataframe to get data to be converted into JSON. This overrides json_source_table_name
        self.json_target_write_method=json_target_write_method # write modes ("overwrite"|"append")
        self.jsondata_file_format=jsondata_file_format # Fileformat Options: (json | avro | parquet)
        self.jsondata_compression=jsondata_compression # Compression format for the files to be written Options: (gzip | None)
        self.run_mode=run_mode # value "debug" if you don't want to write to table. Options ("debug"|<any thing else>)

class IncrementalConfig():
    def __init__(self,
                 target_table_name,
                 incremental_perist_table=None,
                 incremental_perist_data_path=None,
                 state_table=None,
                 source_reference_tbl=None,
                 source_reference_df=None,
                 nrows=1000,
                 n_gen_partitions=200,
                 incremental_persist_write_mode="append",
                 hive_partitions_limit_override_flag=False,
                 run_mode="dev",
                 partition_col_list=[],
                 incremental_update_ratio=0.1,
                 updatefields=None,
                 update_filter=None,
                 sequencefields=None,
                 load_type="incremental",
                 persist_incrementals=False,
                 insert_filter=None,
                 inc_mode_max=None,
                 update_field_stats_table=None,
                 update_values_limit=100):
        self.target_table_name=target_table_name # table to read data to generate updates
        self.incremental_perist_table=incremental_perist_table # target table to write the incremental generated records
        self.incremental_perist_data_path=incremental_perist_data_path # Table path to write the delta data in case of external locations being used
        self.state_table=state_table
        self.source_reference_tbl=source_reference_tbl # "if custom_source_schema & source_reference_df is not provided, use this to get schema. 
                                                       # If partition_col_list is not provided, use this table to derive partition information". 
                                                       # if colspec_overrides has a rule with "derive": True and if source_reference_df is None, 
                                                       # use this table to derive the values for the derived columns
        self.source_reference_df=source_reference_df # if custom_schema is not provided, use this dataframe derived in the entry point code to derive customer schema
        self.nrows=nrows # number of rows to be generated
        self.n_gen_partitions=n_gen_partitions # number of spark partitions to be generated
        self.incremental_persist_write_mode=incremental_persist_write_mode # options ("overwrite"|"append")
        self.hive_partitions_limit_override_flag=hive_partitions_limit_override_flag  # options (False|True)
        self.run_mode=run_mode # value "debug" if you don't want to write to table. Options ("debug"|<any thing else>)
        self.partition_col_list = partition_col_list # List of partition columns. This will override "schema_source_reference" table partition columns
        self.incremental_update_ratio=incremental_update_ratio # Ratio of incremental records that are updates.
        self.updatefields=updatefields # specify the fields to be updated as part of the incremental updates batch of records
        self.update_filter=update_filter # filter criteria to apply to "source_data_table_name" when pulling records to generate updates
        self.sequencefields=sequencefields # sequence fields are used to APPLY CHANGES during incremental load
                                           # Typically sequencefields are int or timestamp. Specify start/end values for sequencefields for inserts & updates set of records
        self.load_type=load_type #incremental by "default"
        self.persist_incrementals=persist_incrementals # whether to persist incremental data in delta format before writing to json bronze
        self.insert_filter=F.lit(True) if insert_filter is None else insert_filter # Filter on target_table_name to get max primary key
        self.inc_mode_max=inc_mode_max # Dict of key = primary_key, value = max value of pk for previous run. Incremental values will be generated after this value
        self.update_field_stats_table=update_field_stats_table # table to capture distinct field values for generated updated values for the field
        self.update_values_limit=update_values_limit # max number of values for field to use for generating updates
