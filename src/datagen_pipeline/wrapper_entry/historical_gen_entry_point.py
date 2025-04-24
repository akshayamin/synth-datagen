# from example_configs.template import aa_catalog_silver_demo  # Assuming your config class is defined in your_module
from datagen_wrapper import baseconfig
from pyspark.sql import SparkSession
# import configs.example_configs.common_gen_config as common_config
# import configs.example_configs.historical_gen_config as historical_config
from datagen_wrapper import dg_wrapper as dgw
import dbldatagen as dg

def main():
    ##### Main Code & User Flow #####
    spark = SparkSession.builder.appName("DataGeneration").getOrCreate()

    print("\n\n\n*************** STARTING HISTORICAL LOAD  ***************")

    ############################################################################################################
    ## OPTION 1: Pass Configs Directly while creating HistoricalConfig class object
    ############################################################################################################
    myCommonConfig=baseconfig.CommonConfig(
        spark,
        default_colspecs={
            "StringType": {"template": r"\\w \\w|\\w a. \\w"},
            "IntegerType": {"minValue": 1, "maxValue": 1000000},
            "DateType": { "data_range":dg.DateRange('2021-10-01','2022-01-31' , 'days=3', datetime_format='%Y-%m-%d')},},
        colspec_overrides = {
        # "category": { "derive": True, "type": "categorical", "field": "category"},
        # "age":{ "derive": True, "type": "continuous", "field": "age", "percentNulls": 0.1},
        # "age":{ "derive": True, "type": "continuous", "field": "ld_multiplier_3", "percentNulls": 0.1},
        "ld_multiplier_2": {"step":0.5,"minValue": 1.0, "maxValue":25.0,},
        },
        primary_key=["pid_int", "pid_str"]
    )
    gen_data = dgw.Gendata(myCommonConfig)
    
    # source_reference_df=spark.sql("select * from aa_catalog.org1.silver_testtable_16")

    myHistoricalConfig=baseconfig.HistoricalConfig(
        source_reference_tbl="aa_catalog.datagen.ref_table", 
        target_table_name="aa_catalog.synth.tbl_v1", 
        nrows=10,
        n_gen_partitions=1, 
        write_mode="overwrite",
        # source_reference_df=source_reference_df
        )
    
    ############################################################################################################
    ## OPTION 2: Read from historical_gen_config.py and pass values to HistoricalConfig class object
    ############################################################################################################

    # Instantiate CommonConfig with the combined configuration
    # myCommonConfig = baseconfig.CommonConfig(spark, **common_config.get_config())
    # gen_data = dgw.Gendata(myCommonConfig)
    
    # source_reference_df=spark.sql("select * from aa_catalog.org1.silver_testtable_16")

    # Instantiate HistoricalConfig with the combined configuration
    # myHistoricalConfig = baseconfig.HistoricalConfig(
    #     **historical_config.get_config(),
    #     # source_reference_df=source_reference_df
    # )

    ############################################################################################################
    # Generate Data (run load)
    ############################################################################################################
    print(f"""Starting Historical Load:
          Deriving stats from table-{myHistoricalConfig.source_reference_tbl} or dataframe-source_reference_df
          Writing {myHistoricalConfig.nrows} to Table-{myHistoricalConfig.target_table_name}, Partitions-{str(myHistoricalConfig.partition_col_list)}
          Write Mode: {myHistoricalConfig.write_mode}""")
    
    gen_hist_df = gen_data.generate_data(myHistoricalConfig)
    gen_hist_df.show()

    print("\n\n\n*************** ENDING HISTORICAL LOAD  ***************")

if __name__ == "__main__":
    main()



