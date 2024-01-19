from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import pandas as pd

class GroupByIdVisit(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
        
        grouped_by_id_df = data.input_data.groupby('idvisit').agg({
            'idlink_va': pd.Series.nunique,
            'server_time': ['min', 'max'],
        })
        
        grouped_by_id_df.columns = ['idlink_va_count', 'first_event_time', 'last_event_time']
        
        total_time = (grouped_by_id_df['last_event_time'] - grouped_by_id_df['first_event_time']).dt.total_seconds()
        grouped_by_id_df = grouped_by_id_df.assign(
            avg_time=total_time / grouped_by_id_df['idlink_va_count'],
            total_time=total_time
        )
                
        data.grouped_by_idvisit = grouped_by_id_df.reset_index().sort_values(by='idlink_va_count', ascending=False)
        
        return data
