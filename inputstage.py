from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import pandas as pd


class InputStage(AbstractUsageStatsPipelineStage):
    
    def __init___(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
    
        file = self._configContext._config['INPUT_STAGE']['PARQUET_FILENAME']
        
        df = pd.read_parquet(file, 
                     columns=['idvisit', 'idsite', 'idlink_va', 'location_country', 'location_city', 'location_ip', 'url', 'server_time', 'custom_var_value1', 'custom_var_value2',
                              'custom_var_value6', 'custom_var_value8', 'type', 'location_latitude', 'location_longitude', 'level'])

        data.input_data = df
        return data