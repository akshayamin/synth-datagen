from pyspark.sql import SparkSession
from datagen_wrapper import baseconfig
import example_configs.json_gen_config as json_config
from datagen_wrapper import dg_wrapper as dgw

def main():
    ##### Main Code & User Flow #####
    spark = SparkSession.builder.appName("DataGeneration").getOrCreate()

    print("\n\n\n*************** STARTING JSONIFY LOAD  ***************")
    
    ############################################################################################################
    ## OPTION 1: Pass Configs Directly while creating JsonifyConfig class object
    ############################################################################################################
    myCommonConfig=baseconfig.CommonConfig(spark=spark)
    gen_data = dgw.Gendata(myCommonConfig)

    # Optionally read into dataframe and pass df as source to be jsonified
    # source_df = spark.sql("select * from aa_catalog.org1.silver_57")

    myJsonConfig=baseconfig.JsonifyConfig(
        json_target_mapper_str={
                "header": ["pid_str", {"transaction": ["email", "category"]}],
                "paycore": ["ld_multiplier_1"],
            },
        json_target_path="s3://databricks-akshayamin/data/json/s102/",
        additional_root_fields_to_extract=["station_id", "test_id"],
        json_source_table_name="aa_catalog.org1.silver_102",
        json_target_write_method="append",
        jsondata_file_format="json",
        run_mode = "dev",
        # json_source_df=source_df
        )
    
    ############################################################################################################
    ## OPTION 2: Read from json_gen_config.py and pass values to JsonifyConfig class object
    ############################################################################################################

    # Instantiate CommonConfig with the combined configuration
    myCommonConfig = baseconfig.CommonConfig(spark=spark)
    gen_data = dgw.Gendata(myCommonConfig)
    
    # Optionally read into dataframe and pass df as source to be jsonified
    # source_df = spark.sql("select * from aa_catalog.org1.silver_57")

    # Instantiate JsonifyConfig with the combined configuration
    myJsonConfig = baseconfig.JsonifyConfig(
        **json_config.get_config(),
        # json_source_df=source_df
    )


    ############################################################################################################
    # Generate Json Data
    ############################################################################################################

    print(f"""Starting Jsonify Load:
          Deriving stats from table-{myJsonConfig.json_source_table_name} or dataframe-source_df
          Writing data to Path-{myJsonConfig.json_target_path}
          Write Mode- {myJsonConfig.json_target_write_method}
          Write Format- {myJsonConfig.jsondata_file_format}""")
    
    gen_jsonify_df = gen_data.jsonify_generated_data(myJsonConfig)
    gen_jsonify_df.show()

    print("\n\n\n*************** END JSONIFY LOAD  ***************")

if __name__ == "__main__":
    main()
