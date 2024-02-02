from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import pandas as pd


class InputStage(AbstractUsageStatsPipelineStage):
    
    def __init___(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
    
        visits_file = self._configContext._config['INPUT_STAGE']['VISITS_PARQUET_FILENAME']
        events_file = self._configContext._config['INPUT_STAGE']['EVENTS_PARQUET_FILENAME']
        
      
        data.visits_df = pd.read_parquet(visits_file, 
                                    columns=['idvisit', 'visit_last_action_time', 'visit_first_action_time', 
                                             'visit_total_actions', 
                                             'location_country'])
        
        data.events_df = pd.read_parquet(events_file,
                                    columns=['idlink_va', 'idvisit', 'idaction_url_ref', 'idaction_name_ref', 'server_time',
                                             'pageview_position', 'custom_var_v1',
                                             'custom_var_v3', 'action_type', 'action_url', 'action_url_prefix'])
        


        return data
  


     