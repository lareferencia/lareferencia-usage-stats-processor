from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext


class RobotsFilterStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
                
        visits_df = data.visits_df
        
        total_time = (visits_df['visit_last_action_time'] - visits_df['visit_first_action_time']).dt.total_seconds()
        visits_df = visits_df.assign(
            avg_action_time=total_time / visits_df['visit_total_actions'],
            total_time=total_time
        )
    
        mask = self._configContext._config['ROBOTS_FILTER_STAGE']['QUERY_STR']
        visits_df = visits_df.query(mask)
        data.visits_df = visits_df
        
        events_df = data.events_df[data.events_df['idvisit'].isin(visits_df['idvisit'])]
        data.events_df = events_df
        
                
        return data
     
 
        