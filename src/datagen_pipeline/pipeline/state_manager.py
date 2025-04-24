from datetime import timedelta, datetime, time
from pyspark.sql.functions import lit, col
from pyspark.sql import functions as F
from pipeline.dyno_config import DynoConfig

class StateManager:

    def __init__(
            self,
            spark,
            state_table_name,
            updates_per_day,
            days_in_update_range,
            gen_partitions,
            incremental_record_count,
            resume,
            dynoConfig: DynoConfig,
            persist_freq=10
        ):
        self.spark = spark
        self.state_table_name = state_table_name
        self.updates_per_day = updates_per_day
        self.days_in_update_range = days_in_update_range
        self.gen_partitions = gen_partitions
        self.incremental_record_count = incremental_record_count
        self.resume = resume
        self.dynoConfig = dynoConfig
        self.persist_freq = persist_freq
        self.update_counter = 1
        self.state = {}

    def initialize_state(self):
        print("Initializing state from config")
        self.state["update_count"] = self.update_counter
        self.state["state_date"] = self.dynoConfig.globals.FROM_DATE
        self.state["primordial_date"] = self.dynoConfig.globals.FROM_DATE
        self.state["incremental_record_count"] = self.incremental_record_count
        self.state["gen_partitions"] = self.gen_partitions
        self.state["updates_per_day"] = self.updates_per_day
        self.state["days_in_update_range"] = self.days_in_update_range
        self.state["insert_from_ts"] = str(self.get_insert_from_ts())
        self.state["insert_until_ts"] = str(self.get_insert_until_ts())
        self.state["update_from_ts"] = str(self.get_update_from_ts())
        self.state["update_until_ts"] = str(self.get_update_until_ts())

    def date_progression(self):
        return self.update_counter // self.updates_per_day
        # return int(round((self.update_counter / self.updates_per_day) - 0.51))
    
    def increment_date(self, inc_d, n):
        return inc_d + timedelta(n)
    
    def increment_state_date(self):
        self.state["state_date"] = max(self.increment_date(self.state["primordial_date"], self.date_progression()), self.state["state_date"])

    def get_update_from_ts(self):
        return datetime.combine(self.increment_date(self.state["state_date"], -(self.days_in_update_range - 1)), time(00, 00, 00))

    def get_update_until_ts(self):
        return datetime.combine(self.state["state_date"], time(23, 59, 59))
    
    def get_insert_from_ts(self):
        return datetime.combine(self.state["state_date"], time(00, 00, 00))
    
    def get_insert_until_ts(self):
        return datetime.combine(self.state["state_date"], time(23, 59, 59))
    
    def persist_state(self, overwrite=False):
        state_df = self.spark.createDataFrame([self.state])\
            .withColumn("__synth_state_ts", F.unix_timestamp(F.current_timestamp())).coalesce(1)
        if overwrite:
            state_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(self.state_table_name)
        else:
            state_df.write.format("delta").mode("append").saveAsTable(self.state_table_name)

    def update_state(self):
        self.update_counter += 1
        self.state["update_count"] = self.update_counter
        self.increment_state_date()
        self.state["insert_from_ts"] = str(self.get_insert_from_ts())
        self.state["insert_until_ts"] = str(self.get_insert_until_ts())
        self.state["update_from_ts"] = str(self.get_update_from_ts())
        self.state["update_until_ts"] = str(self.get_update_until_ts())
        if self.update_counter % self.persist_freq == 0:
            self.persist_state()

    def set_state(self, state, persist=True):
        self.state = state
        if persist:
            self.persist_state()

    def reset_state(self, drop=False):
        if self.spark.catalog.tableExists(self.state_table_name):
            self.spark.sql(f"TRUNCATE TABLE {self.state_table_name}")
        if drop:
            self.spark.sql(f"DROP TABLE IF EXISTS {self.state_table_name}")
        self.state = {}
        print("State reset")

    def initialize_persisted_state(self):
        if self.resume and\
            self.spark.catalog.tableExists(self.state_table_name) and not\
            self.spark.table(self.state_table_name).count() == 0:
            print("Retrieving state from persisted table")
            self.state = self.spark.table(self.state_table_name) \
                .orderBy(col("__synth_state_ts").desc()) \
                .first().asDict()
            # self.state["run_start_state_date"] = self.state["state_date"]
            self.state["incremental_record_count"] = self.incremental_record_count
            self.state["gen_partitions"] = self.gen_partitions
            self.state["updates_per_day"] = self.updates_per_day
            self.state["days_in_update_range"] = self.days_in_update_range
            self.update_counter = self.state["update_count"]
        else:
            self.initialize_state()
            self.get_current_state()

    def get_current_state(self):
        if not self.state:
            self.initialize_persisted_state()
        return self.state












