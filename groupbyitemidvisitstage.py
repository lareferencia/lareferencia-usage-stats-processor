from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext


class GroupByItemIdvisitStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
        
        # initialize the views, outlinks and downloads columns    
        data.events_df['views'] = 0
        data.events_df['outlinks'] = 0
        data.events_df['downloads'] = 0
        
        # set the views, outlinks and downloads columns to 1 if the action_type is 1, 2 or 3 respectively
        data.events_df.loc[data.events_df['action_type'] == 1, 'views'] = 1
        data.events_df.loc[data.events_df['action_type'] == 2, 'outlinks'] = 1
        data.events_df.loc[data.events_df['action_type'] == 3, 'downloads'] = 1
        
        # group by idvisit and oai_identifier and sum the views, outlinks and downloads columns
        data.events_df = data.events_df.groupby(['idvisit', 'oai_identifier']).agg({
            'views': 'max', 
            'outlinks': 'max', 
            'downloads': 'max', 
        }).reset_index()
        
        return data       


