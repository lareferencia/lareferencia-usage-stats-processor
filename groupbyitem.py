from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext




class GroupByItem(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
        
        events_df = data.events_df
        events_df['conversiones'] = (events_df['views'] == 1) & (events_df['downloads'] == 1).astype(int)
        
        df_grouped_by_item = events_df.groupby('custom_var_v1').agg({
            'views': 'sum', 
            'outlinks': 'sum', 
            'downloads': 'sum', 
            'conversiones': 'sum'

        }).reset_index()
        
        # print(df_grouped_by_item.sort_values(by=['conversiones'], ascending=False).head(10))
        
        data.events_by_item = df_grouped_by_item
    
        return data       
