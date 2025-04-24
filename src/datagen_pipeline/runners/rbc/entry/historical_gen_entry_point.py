# from example_configs.template import aa_catalog_silver_demo  # Assuming your config class is defined in your_module
from datagen_wrapper import baseconfig
from pyspark.sql import SparkSession
# import configs.example_configs.common_gen_config as common_config
# import configs.example_configs.historical_gen_config as historical_config
from datagen_wrapper import dg_wrapper as dgw
import dbldatagen as dg
from dbldatagen import fakerText
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType

def main():
    ##### Main Code & User Flow #####
    spark = SparkSession.builder.appName("DataGeneration").getOrCreate()

    print("\n\n\n*************** STARTING HISTORICAL LOAD  ***************")

    country_codes = [
        "CN", "US", "FR"
    ]

    country_weights = [
        3,1,1
    ]

    ############################################################################################################
    ## OPTION 1: Pass Configs Directly while creating HistoricalConfig class object
    ############################################################################################################
    myCommonConfig=baseconfig.CommonConfig(
        spark,
        custom_source_schema=StructType([StructField('pid', IntegerType(), True), StructField('code1', IntegerType(), True), StructField('code2', IntegerType(), True), StructField('code3', IntegerType(), True), StructField('code4', IntegerType(), True), StructField('site_cd', StringType(), True), StructField('sector_status_desc', StringType(), True), StructField('categories', StringType(), True), StructField('test_cell_flg', IntegerType(), True)]),
        default_colspecs={
            "StringType": {"template": r"\\w \\w|\\w a. \\w"},
            "IntegerType": {"minValue": 90, "maxValue": 99},
            "DateType": { "data_range":dg.DateRange('2021-10-01','2022-01-31' , 'days=3', datetime_format='%Y-%m-%d')},},
        colspec_overrides = 
        # None,
        {
        "derived_categories": {"derive": True, "type": "categorical", "field": "categories"},
        "dervied_flags": {"derive": True, "type": "categorical", "field": "test_cell_flg"},
        "ld_multiplier_2": {"step": 0.5, "minValue": 1.0, "maxValue": 25.0},
        "email": {"text": fakerText("ascii_company_email")},
        "account_id_with_prefix": {"minValue": 1000000, "maxValue": 10000000, "prefix": "a", "random": True},
        "site_cd": {"prefix": "site", "baseColumn": "pid"},
        "sector_technology_desc": {"values": ["GSM", "UMTS", "LTE", "UNKNOWN"], "random": True},
        "test_cell_flg": {"values": [0, 1], "random": True},
        "email_custom": {"template": r'\w.\w@rbc.com'},
        "address": {"text": fakerText("address")},
        "city": {"text": fakerText("city")},
        "zip": {"text": fakerText("postcode")},
        "country": {"values": country_codes, "weights": country_weights},
        "internal_device_id": {"minValue": 0x1000000000000, "uniqueValues": 10, "omit": False, "baseColumnType": "hash"},
        "unique_device_id": {"format": "0x%013x", "baseColumn": "pid"}
        },
        primary_key=["pid"],
        compute_stats_table="aa_catalog.datagen.compute_stats_table"
    )
    gen_data = dgw.Gendata(myCommonConfig)
    
    # source_reference_df=spark.sql("select * from aa_catalog.org1.silver_testtable_16")

    myHistoricalConfig=baseconfig.HistoricalConfig(
        source_reference_tbl="aa_catalog.datagen.rbc_refdata",
        target_table_name="aa_catalog.synth.rbc_gendata", 
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



