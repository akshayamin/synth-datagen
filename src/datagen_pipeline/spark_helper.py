from databricks.connect import DatabricksSession
from databricks.sdk.core import Config
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()
if bool(os.environ.get("IS_DEV_LOCAL")):
    print("IS LOCAL")
    config = Config(
    profile    = "e2-demo-west",
    cluster_id = "0329-122330-8r4cxhau"
    )
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
else:
    print("NOT LOCAL")
    spark = SparkSession.builder.getOrCreate()

