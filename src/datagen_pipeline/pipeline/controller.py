import dbldatagen as dg
from datagen_wrapper import dg_wrapper as dgw
from datagen_wrapper import baseconfig
from pipeline.state_manager import StateManager
from pyspark.sql.functions import lit, col
from pipeline.dyno_config import DynoConfig
from utils import toolbelt
import copy

class IncDateController:
    def __init__(
            self,
            spark,
            dynoConfig: DynoConfig,
            date_control_field,
            updates_per_day,
            update_days_range_count,
            incremental_record_count,
            resume = True,
            state_persist_freq = 20
    ):
        self.spark = spark
        self.dynoConfig = dynoConfig
        self.date_control_field = date_control_field
        self.updates_per_day = updates_per_day
        self.update_days_range_count = update_days_range_count
        self.incremental_record_count = incremental_record_count
        self.state_table_name = dynoConfig.incremental.state_table
        self.resume = resume
        self.state_persist_freq = state_persist_freq
        self.common_config = dynoConfig.common
        self.initial_common_config = dynoConfig.common
        self.inc_mode_max_keys = {}
        self.gen_partitions = toolbelt.optimize_partitions(self.incremental_record_count, dynoConfig.globals.TARGET_ROWS_PER_PARTITION)
        
        self.state_manager = StateManager(
            self.spark,
            self.state_table_name,
            self.updates_per_day,
            self.update_days_range_count,
            self.gen_partitions,
            self.incremental_record_count,
            self.resume,
            self.dynoConfig,
            self.state_persist_freq
        )
        self.state_manager.get_current_state()
    
    def increment_incremental_config(self):
        update_until_date_c = lit(str(self.state_manager.get_update_until_ts().date()))
        updaet_from_date_c = lit(str(self.state_manager.get_update_from_ts().date()))

        update_until_ts_c = update_until_date_c.cast("timestamp")
        update_from_ts_c = updaet_from_date_c.cast("timestamp")

        inc_filter = (
            col(self.date_control_field).between(updaet_from_date_c, update_until_date_c) &
            col("tx_ts").between(update_from_ts_c, update_until_ts_c)
        )

        new_inc_config = self.dynoConfig.incremental
        new_inc_config.update_filter = inc_filter
        new_inc_config.sequencefields[self.date_control_field]["insertstart"] = str(self.state_manager.get_insert_from_ts().date())
        new_inc_config.sequencefields[self.date_control_field]["insertend"] = str(self.state_manager.get_insert_until_ts().date())
        new_inc_config.sequencefields[self.date_control_field]["updatestart"] = str(self.state_manager.get_update_from_ts().date())
        new_inc_config.sequencefields[self.date_control_field]["updateend"] = str(self.state_manager.get_update_until_ts().date())
        new_inc_config.sequencefields["tx_ts"]["insertstart"] = str(self.state_manager.get_insert_from_ts())
        new_inc_config.sequencefields["tx_ts"]["insertend"] = str(self.state_manager.get_insert_until_ts())
        new_inc_config.sequencefields["tx_ts"]["updatestart"] = str(self.state_manager.get_update_from_ts())
        new_inc_config.sequencefields["tx_ts"]["updateend"] = str(self.state_manager.get_update_until_ts())
        new_inc_config.nrows = self.incremental_record_count
        new_inc_config.n_gen_partitions = self.gen_partitions

        self.dynoConfig.incremental = new_inc_config
        return self.dynoConfig.incremental
        # return baseconfig.IncrementalConfig(**new_inc_config.get_config())
    
    def increment_common_config(self):
        new_dt_ctrl_field_spec = { "data_range": dg.DateRange(str(self.state_manager.get_insert_from_ts().date()), str(self.state_manager.get_insert_until_ts().date()), 'days=1', datetime_format='%Y-%m-%d') }
        new_common_config = self.dynoConfig.common
        new_col_spec_overrides = self.dynoConfig.common.colspec_overrides
        new_default_col_spec_overrides = self.dynoConfig.common.default_colspecs
        new_col_spec_overrides[self.date_control_field] = new_dt_ctrl_field_spec
        new_default_col_spec_overrides["DateType"] = new_dt_ctrl_field_spec
        new_common_config.colspec_overrides = new_col_spec_overrides
        new_common_config.default_colspecs = new_default_col_spec_overrides

        self.dynoConfig.common = new_common_config
        return self.dynoConfig.common
        # return dgw.Gendata(baseconfig.CommonConfig(spark=self.spark, **new_common_config.get_config()))
    
    def execute_incremental(self, dry_run=False):
        gfp_common_config = self.increment_common_config()
        gfp_inc_config = self.increment_incremental_config()

        gen_data = dgw.Gendata(gfp_common_config)
        if not dry_run:
            (_, self.inc_mode_max_keys) = gen_data.generate_incremental_data(gfp_inc_config, self.dynoConfig.jsonify)
        self.state_manager.update_state()
    







