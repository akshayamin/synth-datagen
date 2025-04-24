from configs.tx_example.globals import *
from spark_helper import spark
from pipeline.controller import IncDateController
from pipeline.dyno_config import DynoConfig
import time

UPDATES_PER_DAY = 4
RESUME = False
DAYS_IN_UPDATE_RANGE = 3
TOTAL_UPDATES = 8
INCREMENTAL_RECORD_COUNT = 8000000

dynoConf = DynoConfig(spark)

controller = IncDateController(
    spark,
    dynoConf,
    "tx_date",
    UPDATES_PER_DAY,
    DAYS_IN_UPDATE_RANGE,
    INCREMENTAL_RECORD_COUNT,
    resume=RESUME,
    state_persist_freq=4
)

# def track_runtime(work_func, batch_num, *args, **kwargs):
#     start_time = time.time()
#     print(f"Batch {batch_num} - Start Time: {start_time}")
#     work_func(*args, **kwargs)
#     complete_time = time.time()
#     print(f"Batch {batch_num} - Completion Time: {complete_time}")
#     total_runtime = complete_time - start_time
#     print(f"Batch {batch_num} - Total Runtime: {total_runtime} seconds")

# for i in range(1, TOTAL_UPDATES):
#     track_runtime(controller.execute_incremental, i, dry_run=True)

controller.execute_incremental(dry_run=True)