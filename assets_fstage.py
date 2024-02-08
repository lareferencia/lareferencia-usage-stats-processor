from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import re


class AssetsFilterStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
        self.regex = configContext.getConfig('DOWNLOAD_EVENTS_FILTER_STAGE','REGEX')
        
    def run(self, data: UsageStatsData) -> UsageStatsData:
                 
        # compile the regex pattern              
        regex = re.compile(self.regex)
        
        # function to filter the action_url column based on the regex pattern
        def regex_filter(x):
            return regex.match( str(x)[-9:].lower())
        
        # filter the action_url column based on the regex pattern
        data.events_df = data.events_df[~data.events_df['action_url'].apply(regex_filter).notna()]

        
        return data