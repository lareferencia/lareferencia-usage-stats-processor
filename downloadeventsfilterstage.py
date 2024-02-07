from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import re


class DownloadEventsFilterStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
        
        events_df = data.events_df
                      
        regex = re.compile('^.\.[a-z]{3}\.(jpg|png)$|^\.jpeg\.(jpg|png)$')
        
        def regex_filter(x):
            return regex.match( str(x)[-9:].lower())
        
        data.events_df = events_df[~events_df['action_url'].apply(regex_filter).notna()]

        
        return data