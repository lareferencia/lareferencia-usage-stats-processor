from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext


class RobotsFilterStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
        
        # get the mask from the configuration
        self.mask = configContext.getConfig('ROBOTS_FILTER_STAGE','QUERY_STR')
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
                        
        # add the avg_action_time and total_time columns to the visits dataframe
        total_time = (data.visits_df['visit_last_action_time'] - data.visits_df['visit_first_action_time']).dt.total_seconds()        
        data.visits_df = data.visits_df.assign(
            avg_action_time=total_time / data.visits_df['visit_total_actions'],
            total_time=total_time
        )
    
        # filter the visits dataframe with the mask
        data.visits_df = data.visits_df.query(self.mask)
        
        # filter the events dataframe with the visits dataframe
        data.events_df = data.events_df[data.events_df['idvisit'].isin(data.visits_df['idvisit'])]
        
                
        return data
     
 
        