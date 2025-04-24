from configs.tx_example.globals import *
from spark_helper import spark
from pipeline.controller import IncDateController
from pipeline.dyno_config import DynoConfig
import datetime

UPDATES_PER_DAY = 4
RESUME = True
DAYS_IN_UPDATE_RANGE = 3
TOTAL_UPDATES = 8
INCREMENTAL_RECORD_COUNT = 24000
PERSIST_STATE_FREQ = 2

dynoConf = DynoConfig(spark)

##  IMPORTANT: As of the latest update, if you intend to use the pipeline controller you MUST update the the pipeline.controller.py 
##    file to update the correct fields.
##    Updates required are inc_filter and sequencefields in increment_incremental_config function
controller = IncDateController(
    spark,
    dynoConf,
    "tx_date",
    UPDATES_PER_DAY,
    DAYS_IN_UPDATE_RANGE,
    INCREMENTAL_RECORD_COUNT,
    resume=RESUME,
    state_persist_freq=PERSIST_STATE_FREQ
)

def track_runtime(work_func, batch_num, *args, **kwargs):
    start_time = datetime.datetime.now()
    print(f"Batch {batch_num} - Start Time: {start_time.strftime("%Y-%m-%d %H:%M:%S")}")
    work_func(*args, **kwargs)
    complete_time = datetime.datetime.now()
    print(f"Batch {batch_num} - Completion Time: {complete_time.strftime("%Y-%m-%d %H:%M:%S")}")
    total_runtime = (complete_time - start_time).total_seconds()
    print(f"Batch {batch_num} - Total Runtime: {total_runtime} seconds")

for i in range(1, TOTAL_UPDATES):
    track_runtime(controller.execute_incremental, i, dry_run=True)