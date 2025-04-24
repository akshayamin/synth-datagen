from pyspark.sql import SparkSession
from datagen_wrapper import baseconfig
import example_configs.incremental_gen_config as incremental_config
import example_configs.common_gen_config as common_config
from datagen_wrapper import dg_wrapper as dgw
from datagen_wrapper import gendatafactory as gdf
from pyspark.sql.functions import col
import example_configs.json_gen_config as json_config
import dbldatagen as dg
import pyspark.sql.functions as F

def main():
    ##### Main Code & User Flow #####
    spark = SparkSession.builder.appName("DataGeneration").getOrCreate()

    print("\n\n\n*************** STARTING INCREMENTAL LOAD  ***************")

    ############################################################################################################
    ## OPTION 1: Pass Configs Directly while creating HistoricalConfig class object
    ############################################################################################################
    # myCommonConfig=baseconfig.CommonConfig(
    #     spark=spark, 
    #     default_colspecs={
    #         "StringType": {"template": r"\\w \\w|\\w a. \\w"},
    #         "IntegerType": {"minValue": 1, "maxValue": 1000000},
    #         "DateType": { "data_range":dg.DateRange('2021-10-01','2022-01-31' , 'days=3', datetime_format='%Y-%m-%d')},},
    #     colspec_overrides = {
    #     "category": { "derive": True, "type": "categorical", "field": "category"},
    #     "ld_multiplier_3":{ "derive": True, "type": "continuous", "field": "ld_multiplier_3", "percentNulls": 0.1, "step":1},
    #     },
    #     primary_key=["pid"]
    # )
    # gen_data = dgw.Gendata(myCommonConfig)
    
    # # source_reference_df = spark.sql("select * from aa_catalog.org1.silver_101")

    # myIncrementalConfig=baseconfig.IncrementalConfig(
    #     target_table_name="aa_catalog.org1.gentable_1",
    #     source_reference_tbl="aa_catalog.org1.silver_21", 
    #     incremental_perist_table="aa_catalog.org1.gentable_1_inc",
    #     incremental_persist_write_mode="append",
    #     # source_reference_df=source_reference_df,
    #     persist_incrementals=True,
    #     nrows=20, 
    #     n_gen_partitions=2, 
    #     partition_col_list=["category"],
    #     incremental_update_ratio=0.1,
    #     updatefields=["email", "name"],
    #     update_filter = F.expr("modified_ts BETWEEN '2023-01-01 00:00:00 ' AND '2023-12-31 00:00:00' AND ld_multiplier_1 BETWEEN 1 AND 5"),
    #     # update_filter = F.col("category") == 'A_sfix',
    #     sequencefields={
    #         "modified_ts": {"insertstart": '2024-06-01 00:00:00', "insertend": '2024-06-30 00:00:00', "updatestart": '2025-06-01 00:00:00',"updateend": '2025-06-30 00:00:00'},
    #         "sequence_field": {"insertstart": 2000, "insertend": 3000, "updatestart": 1000,"updateend": 2000},
    #         })

    # myJsonConfig=baseconfig.JsonifyConfig(
    #     json_target_mapper_str={
    #             "header": ["pid", {"transaction": ["email", "category"]}],
    #             "paycore": ["ld_multiplier_1"],
    #         },
    #     json_target_path="s3://databricks-akshayamin/data/json/bronze_102/",
    #     additional_root_fields_to_extract=["station_id", "test_id"],
    #     json_target_write_method="append",
    #     jsondata_file_format="json",
    #     run_mode = "dev"
    #     )
    
    ############################################################################################################
    ## OPTION 2: Read from incremental_gen_config.py and pass values to IncrementalConfig class object
    ############################################################################################################

    # Instantiate CommonConfig with the combined configuration
    myCommonConfig = baseconfig.CommonConfig(
        spark=spark, **common_config.get_config()
    )
    gen_data = dgw.Gendata(myCommonConfig)
    
    # source_reference_df=spark.sql("select * from aa_catalog.org1.silver_102")

    # Instantiate HistoricalConfig with the combined configuration
    myIncrementalConfig = baseconfig.IncrementalConfig(
        **incremental_config.get_config(),
        # source_reference_df=source_reference_df
    )

    myJsonConfig = baseconfig.JsonifyConfig(
        **json_config.get_config()
    )

    ############################################################################################################
    # Generate Incremental Data (run load)
    ############################################################################################################
    print(f"""Starting Incremental Load:
          Deriving stats from table-{myIncrementalConfig.source_reference_tbl} or dataframe-source_reference_df
          Writing {myIncrementalConfig.nrows} to Table-{myIncrementalConfig.incremental_perist_table}, Partitions-{str(myIncrementalConfig.partition_col_list)}
          Write Mode: {myIncrementalConfig.incremental_persist_write_mode}
        
        Incremental Generated data will be written as JSON:
          Writing data to Path-{myJsonConfig.json_target_path}
          Write Mode- {myJsonConfig.json_target_write_method}
          Write Format- {myJsonConfig.jsondata_file_format}""")

    (gen_incremental_df, inc_mode_max_next) = gen_data.generate_incremental_data(myIncrementalConfig, myJsonConfig)
    gen_incremental_df.show()
    print("inc_mode_max: " + str(inc_mode_max_next))

    # myIncrementalConfig = baseconfig.IncrementalConfig(
    #     **incremental_config.get_config(),
    #     inc_mode_max=inc_mode_max_next
    # )
    # (gen_incremental_df, inc_mode_max_next) = gen_data.generate_incremental_data(myIncrementalConfig, myJsonConfig)
    # # gen_incremental_df.show()
    # print("inc_mode_max: " + str(inc_mode_max_next))

    print("\n\n\n*************** END INCREMENTAL LOAD  ***************")

if __name__ == "__main__":
    main()
