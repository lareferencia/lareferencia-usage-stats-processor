from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext


class RobotsFilterStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
        
        mask = self._configContext._config['ROBOTS_FILTER_STAGE']['QUERY_STR']
        
        data.grouped_by_idvisit = data.grouped_by_idvisit.query(mask)
        
        mask = data.grouped_by_idvisit['idvisit'].values
        data.input_data = data.input_data.query('idvisit in @mask')
        
                
        return data
    
     