########################################################################################################################################
## REFER baseconfig.py for detailed list of variables
#######################################################################################################################################
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType, DecimalType, DateType
import dbldatagen as dg
import dbldatagen.distributions as dist
import decimal

# provide schema for data to be generated. If pointing to table for schema, make this None
custom_source_schema = StructType([
    StructField("pid_int", IntegerType(), True),
    StructField("pid_str", StringType(), True),
    StructField("ld_multiplier_1", IntegerType(), True),
    StructField("ld_multiplier_2", DoubleType(), True),
    StructField("ld_multiplier_3", DoubleType(), True),
    StructField("name", StringType(), True),
    StructField("station_id", StringType(), True),    
    StructField("test_id", StringType(), True),    
    StructField("category", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("score", DecimalType(17, 2), True),
    StructField("modified_ts", TimestampType(), True),
    StructField("sequence_field", LongType(), True),
    StructField("sequence_date", DateType(), True),
    StructField("new_name", StringType(), True),
    StructField("new_name_2", StringType(), True),
    StructField("new_name_3", StringType(), True),
    StructField("new_name_4", StringType(), True),
    # Add more fields as needed
])
primary_key=["pid_int", "pid_str"] # primarykey if exists generates unique values. If no, PK. make this None
# default specifications for different datatypes
default_colspecs = { 
"StringType": {"template": r"\\w \\w|\\w a. \\w"},
"IntegerType": {"minValue": 1, "maxValue": 1000000},
"DateType": { "data_range":dg.DateRange('2021-10-01','2022-01-31' , 'days=3', datetime_format='%Y-%m-%d')},
}
# override specs for specific columns as needed. "derive": True will derive values from given field from source_reference_tbl or source_reference_df
colspec_overrides = {
    "category":       { "derive": True, "type": "categorical", "field": "category"},
    # "name":       { "derive": True, "type": "categorical", "field": "name"},
    # "email":          { "template" : r"\\w.\\w@\\w.com"},
    "email":          { "derive": True, "type": "categorical", "field": "email"},
    "email_3":          { "template" : r"\\w.\\w@\\w.com"},
    "modified_ts":    { "expr": "current_timestamp()"},
    "ld_multiplier_1":{ "derive": True, "type": "continuous", "field": "ld_multiplier_1", "percentNulls": 0.1},
    "score":{ "derive": True, "type": "continuous", "field": "score"},
    "sequence_field": {"distribution":dist.Beta(0.5, 0.5), "step":1,"minValue": 1, "maxValue":84,},
    "age":{"minValue":1, "maxValue": 100, "random": True, "distribution": dist.Exponential(1.5)},
    "age4":{"minValue":1, "maxValue": 100, "random": True, "distribution": dist.Exponential(1.5)},
}

# Derived stats computation for categorical & continuous values
compute_stats_table="aa_catalog.org1.datagen_derived_stats_v8" # delta table names to store derived field stats

def get_config():
    # Define your configuration directly in a dictionary.
    config = {
        "custom_source_schema": custom_source_schema,
        "primary_key": primary_key,
        "default_colspecs": default_colspecs,
        "colspec_overrides": colspec_overrides,
        "compute_stats_table": compute_stats_table
    }
    return config
