from configs.tx_example.globals import *
from concurrent.futures import ThreadPoolExecutor
from spark_helper import spark
from databricks.sdk.runtime import dbutils

## All tables in globals are defined here except the source refernece table
##  We don't want to drop that as that's likely a reference or real table.
TABLES_IN_SCOPE = [
    HISTORICAL_LOAD_FLAT_TABLE,
    COMPUTE_STATS_TABLE,
    UPDATE_STATS_TABLE,
    INC_TRACE_TABLE,
    INC_STATE_TABLE
]

def drop_table(table):
    print(f"{table}\n")
    spark.sql(f"DROP TABLE IF EXISTS {table}")

def cleanup_json_files():
    # dbutils = spark.sparkContext._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils()
    dbutils.fs.rm(JSON_TARGET_PATH, True)

def execute_full_reset():
    print("Dropping the following tables:")
    with ThreadPoolExecutor() as executor:
        executor.map(drop_table, TABLES_IN_SCOPE)

    print(f"Cleaning up JSON files at {JSON_TARGET_PATH}")
    cleanup_json_files()