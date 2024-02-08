from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext


class JoinEventsVisitsStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
        
    def run(self, data: UsageStatsData) -> UsageStatsData:

        # merge the events and visits dataframes on the idvisit column
        merged_df = data.events_df.merge(data.visits_df, on='idvisit')
        
        # create a new column called conversions that is 1 if the views and downloads columns are 1, 0 otherwise
        merged_df['conversions'] = ((merged_df['views'] == 1) & (merged_df['downloads'] == 1)).astype(int)

        data.events_df = merged_df
        
        return data       
