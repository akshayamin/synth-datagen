
from configs.tx_example import common_cfg, historical_cfg, incremental_cfg, jsonify_cfg, globals
from configs.tx_example.globals import *
from datagen_wrapper import dg_wrapper as dgw
from datagen_wrapper import baseconfig

## this class will take in and register config objects
##  it is responsible for managing the state of existing configs
##  it will also initialize configs with defaults from the cofnig files
class DynoConfig:
    def __init__(self, spark) -> None:
        self.spark = spark
        self.configs = {}

        self._initialize_default_configs()

    def _initialize_default_configs(self):
        """
        Initialize default configurations from config files.
        """
        self.__dict__["common"] = baseconfig.CommonConfig(self.spark, **common_cfg.get_config())
        self.__dict__["historical"] = baseconfig.HistoricalConfig(**historical_cfg.get_config())
        self.__dict__["jsonify"] = baseconfig.JsonifyConfig(**jsonify_cfg.get_config())
        self.__dict__["incremental"] = baseconfig.IncrementalConfig(**incremental_cfg.get_config())
        self.__dict__["globals"] = globals
