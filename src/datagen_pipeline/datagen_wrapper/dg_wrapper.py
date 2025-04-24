
import dbldatagen as dg
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import logging
from datagen_wrapper import gendatafactory as gdf
import random

logging.basicConfig(level=logging.INFO)

class Gendata:  
    """
    A class to handle data generation and processing using PySpark.
    
    This class utilizes dbldatagen for data generation and additional processing
    methods to tailor the data for specific requirements, such as generating incremental
    data or converting data to JSON format.
    
    Attributes:
        spark (SparkSession): The Spark session instance.
        custom_source_schema (StructType): Schema definition for the data generation.
        primary_key (list): List of columns to be used as primary keys.
        colspec_overrides (dict): Specifications to override default column behaviors.
        default_colspecs (dict): Default specifications for different data types.
        compute_stats_table (str): Table to store computed statistics.
    """

    def __init__(self, 
                 config
                 ) -> None:      
        """
        Initializes Gendata with configuration settings.
        Args:
            config (object): Configuration object containing necessary settings.
        """
        self.spark = config.spark
        self.custom_source_schema = config.custom_source_schema
        self.primary_key=config.primary_key
        if config.colspec_overrides is None:
            self.colspec_overrides = {}
        else:
            self.colspec_overrides = config.colspec_overrides
        if config.default_colspecs is None:
            self.default_colspecs = {}
        else:
            self.default_colspecs = config.default_colspecs
        self.compute_stats_table=config.compute_stats_table

    def generate_data(self, config):
        """
        Generates data based on the configuration provided during initialization.
        
        Args:
            config (object): Configuration object with generation parameters.
            
        Returns:
            DataFrame: The generated DataFrame.
        """
        spark=self.spark
        custom_source_schema=self.custom_source_schema
        primary_key=self.primary_key if self.primary_key is not None else []
        colspec_overrides=self.colspec_overrides
        default_colspecs=self.default_colspecs
        compute_stats_table=self.compute_stats_table
        source_reference_tbl=config.source_reference_tbl
        source_reference_df=config.source_reference_df
        target_table_name=config.target_table_name
        target_data_path=config.target_data_path
        nrows=config.nrows
        n_gen_partitions=config.n_gen_partitions
        load_type=config.load_type
        run_mode=config.run_mode
        partition_col_list=config.partition_col_list
        write_mode=config.write_mode
        hive_partitions_limit_override_flag=config.hive_partitions_limit_override_flag

        gdf.GendataFactory.check_null_variables(
                 {
                     "spark": spark, 
                     "target_table_name": target_table_name, 
                     "nrows": nrows, 
                     "npartitions": n_gen_partitions,
                     "load_type": load_type,
                     "custom_source_schema OR source_reference_df OR source_reference_tbl": None if(custom_source_schema is None and source_reference_tbl is None and source_reference_df is None) else "default"
                 }
        )

        (df_delta, custom_schema)=gdf.GendataFactory.generate_inserts(spark, 
                                                     source_reference_tbl, 
                                                     source_reference_df, 
                                                     target_table_name, 
                                                     custom_source_schema, 
                                                     partition_col_list, 
                                                     colspec_overrides, 
                                                     nrows, 
                                                     n_gen_partitions, 
                                                     primary_key, 
                                                     default_colspecs,
                                                     compute_stats_table)

        df_delta = gdf.GendataFactory.write_delta_format(spark, 
                                                         df_delta, 
                                                         partition_col_list, 
                                                         hive_partitions_limit_override_flag, 
                                                         write_mode, 
                                                         target_data_path, 
                                                         target_table_name, 
                                                         run_mode)

        return df_delta

    def jsonify_generated_data(self, config):
        """
        Converts the generated or specified DataFrame into JSON format.
        
        Args:
            config (object): Configuration object with JSON conversion parameters.
            
        Returns:
            DataFrame: A DataFrame in JSON format.
        """

        spark=self.spark
        json_target_mapper_str=config.json_target_mapper_str
        json_target_path=config.json_target_path
        additional_root_fields_to_extract=config.additional_root_fields_to_extract
        json_source_table_name=config.json_source_table_name
        json_source_df=config.json_source_df
        json_target_write_method=config.json_target_write_method
        jsondata_file_format=config.jsondata_file_format
        jsondata_compression=config.jsondata_compression
        run_mode=config.run_mode
        
        # Null checks for mandatory variables needed
        gdf.GendataFactory.check_null_variables(
            {
                "jsondata_file_format": jsondata_file_format,
                "json_target_mapper_str": json_target_mapper_str, 
                "json_target_path": json_target_path, 
                "json_source_table_name OR json_source_df": None if(json_source_df is None and json_source_table_name is None) else "default"
            }
        )
        json_intermediate_df = gdf.GendataFactory.get_source_data(spark, json_source_df, json_source_table_name)        
        json_df = gdf.GendataFactory.gen_json_struct(json_intermediate_df, json_target_mapper_str, additional_root_fields_to_extract)
        json_df = gdf.GendataFactory.write_json(json_df, jsondata_file_format, json_target_write_method, jsondata_compression, json_target_path, run_mode)
        return json_df


    def generate_incremental_data(self, 
                                  incremental_config,
                                  json_config):
        
        """
        Generates incremental data simulating updates and insertions.
        
        Args:
            incremental_config (object): Configuration for incremental data generation.
            json_config (object): Configuration for converting the output to JSON.
        
        Returns:
            DataFrame: A DataFrame containing the incremental data.
        """
        spark=self.spark
        custom_source_schema=self.custom_source_schema
        primary_key=self.primary_key if self.primary_key is not None else []
        colspec_overrides=self.colspec_overrides
        default_colspecs=self.default_colspecs
        compute_stats_table=self.compute_stats_table
        target_table_name=incremental_config.target_table_name
        incremental_perist_table=incremental_config.incremental_perist_table
        incremental_perist_data_path=incremental_config.incremental_perist_data_path
        source_reference_tbl=incremental_config.source_reference_tbl
        source_reference_df=incremental_config.source_reference_df
        nrows=incremental_config.nrows
        n_gen_partitions=incremental_config.n_gen_partitions
        incremental_persist_write_mode=incremental_config.incremental_persist_write_mode
        hive_partitions_limit_override_flag=incremental_config.hive_partitions_limit_override_flag
        run_mode=incremental_config.run_mode
        partition_col_list=incremental_config.partition_col_list
        incremental_update_ratio=incremental_config.incremental_update_ratio
        updatefields=incremental_config.updatefields
        update_filter=incremental_config.update_filter
        sequencefields=incremental_config.sequencefields
        persist_incrementals=incremental_config.persist_incrementals
        insert_filter=incremental_config.insert_filter
        inc_mode_max=incremental_config.inc_mode_max
        update_field_stats_table=incremental_config.update_field_stats_table
        update_values_limit=incremental_config.update_values_limit

        gdf.GendataFactory.check_null_variables(
                 {
                     "spark": spark, 
                     "target_table_name": target_table_name, 
                     "nrows": nrows, 
                     "npartitions": n_gen_partitions,
                     "custom_source_schema OR source_reference_df OR source_reference_tbl": None if(custom_source_schema is None and source_reference_df is None and source_reference_tbl is None) else "default"
                 }
        )
        gdf.GendataFactory.check_updatefields_not_in_sequencefields(updatefields, sequencefields)
        update_records_count = int(nrows * incremental_update_ratio)
        insert_records = int(nrows - update_records_count)

        (df1_inserts, custom_schema) =gdf.GendataFactory.generate_inserts(spark, 
                                                     source_reference_tbl, 
                                                     source_reference_df, 
                                                     incremental_perist_table, 
                                                     custom_source_schema, 
                                                     partition_col_list, 
                                                     colspec_overrides, 
                                                     insert_records, 
                                                     n_gen_partitions, 
                                                     primary_key, 
                                                     default_colspecs,
                                                     compute_stats_table)
        
        source_data_df = spark.read.table(target_table_name)
        (df1_inserts, next_run_inc_mode_max) =gdf.GendataFactory.make_pk_unique(spark, df1_inserts, source_data_df, primary_key, colspec_overrides, custom_schema, insert_filter, inc_mode_max)
        
        df1_updates = gdf.GendataFactory.generate_updates(spark, update_filter, update_records_count, source_data_df, updatefields, update_field_stats_table, target_table_name, update_values_limit)
        
        df_inc=gdf.GendataFactory.generate_sequence_fields(df1_inserts, df1_updates, sequencefields, custom_schema)
        
        if(persist_incrementals):
            df_inc = gdf.GendataFactory.write_delta_format(spark, df_inc, partition_col_list, hive_partitions_limit_override_flag, incremental_persist_write_mode, incremental_perist_data_path, incremental_perist_table, run_mode)

        json_config.json_source_df=df_inc
        json_df = self.jsonify_generated_data(json_config)
        return (json_df, next_run_inc_mode_max)
