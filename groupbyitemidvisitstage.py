from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext


class GroupByItemIdvisitStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
        
        events_df = data.events_df
        
        events_df['views'] = 0
        events_df['outlinks'] = 0
        events_df['downloads'] = 0
        
        events_df.loc[events_df['action_type'] == 1, 'views'] = 1
        events_df.loc[events_df['action_type'] == 2, 'outlinks'] = 1
        events_df.loc[events_df['action_type'] == 3, 'downloads'] = 1
        
        df_item_idvisit = events_df.groupby(['idvisit', 'oai_identifier']).agg({
            'views': 'max', 
            'outlinks': 'max', 
            'downloads': 'max', 
        }).reset_index()
        
        
        data.events_df = df_item_idvisit
        
        
        
        return data       


