from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import pandas as pd


class RobotsFilterStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
        
        # get the mask from the configuration
        self.QUERY = configContext.getConfig('ROBOTS_FILTER','QUERY_STR')
        self.IDVISIT = configContext.getLabel('ID_VISIT')
    
    def run(self, data: UsageStatsData) -> UsageStatsData:

        if data.visits_df.empty:
            data.events_df = data.events_df.iloc[0:0]
            return data

        data.visits_df['visit_last_action_time'] = pd.to_datetime(data.visits_df['visit_last_action_time'], errors='coerce')
        data.visits_df['visit_first_action_time'] = pd.to_datetime(data.visits_df['visit_first_action_time'], errors='coerce')
        data.visits_df['visit_total_actions'] = pd.to_numeric(data.visits_df['visit_total_actions'], errors='coerce')
                        
        # add the avg_action_time and total_time columns to the visits dataframe
        total_time = (data.visits_df['visit_last_action_time'] - data.visits_df['visit_first_action_time']).dt.total_seconds()        
        data.visits_df = data.visits_df.assign(
            avg_action_time=total_time / data.visits_df['visit_total_actions'].replace(0, pd.NA),
            total_time=total_time
        )
    
        # filter the visits dataframe with the mask
        data.visits_df = data.visits_df.query(self.QUERY)
        
        # filter the events dataframe with the visits dataframe
        data.events_df = data.events_df[data.events_df[self.IDVISIT].isin(data.visits_df[self.IDVISIT])]
        
                
        return data
     
 
        
