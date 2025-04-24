# from example_configs.template import aa_catalog_silver_demo  # Assuming your config class is defined in your_module
from datagen_wrapper import baseconfig
from pyspark.sql import SparkSession
import example_configs.common_gen_config as common_config
import example_configs.historical_gen_config as historical_config
from datagen_wrapper import dg_wrapper_refactor as dgw
from datagen_wrapper import gendatafactory as gdf
from pyspark.sql.functions import col

def main():
    ##### Main Code & User Flow #####
    spark = SparkSession.builder.appName("DataGeneration").getOrCreate()

    # Initialize your configuration instance
    myHistoricalConfig = baseconfig.HistoricalConfig()

    # Load and apply settings from the script
    common_config_values = common_config.get_config()
    historical_config_values = historical_config.get_config()

    print(str(common_config_values))
    print(str(historical_config_values))


    for key, value in common_config_values.items():
        if hasattr(myHistoricalConfig, key):
            setattr(myHistoricalConfig, key, value)
        else:
            print(f"Warning: {key} is not an attribute of HistoricalConfig")

    for key, value in historical_config_values.items():
        if hasattr(myHistoricalConfig, key):
            setattr(myHistoricalConfig, key, value)
        else:
            print(f"Warning: {key} is not an attribute of HistoricalConfig")

    myHistoricalConfig.spark=spark 
    myHistoricalConfig.primary_key = ["pid_str"]
    
    gen_hist_df = dgw.Gendata(myHistoricalConfig).generate_data(myHistoricalConfig)
    gen_hist_df.show()

    print("\n\n\n*************** END HISTORICAL LOAD  ***************")

    

if __name__ == "__main__":
    main()



