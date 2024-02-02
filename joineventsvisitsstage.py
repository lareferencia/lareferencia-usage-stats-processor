from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import pandas as pd
import copy


class JoinEventsVisitsStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:

        empty_entry = { 'views': {}, 'downloads': {}, 'outlinks': {}, 'conversions': {} }
        
        merged_df = data.events_df.merge(data.visits_df, on='idvisit')
           
        data.events_df = merged_df
        
        dict_df = {}
        
        for index, row in merged_df.iterrows():

            entry = row.get('custom_var_v1', copy.deepcopy(empty_entry) )    
            row['custom_var_v1'] = entry

            country = row['localtion_country']
            entry['views'][country] = entry['views'].get(country,0) + row['views']
        

        # def custom_agg(df_gr):
        #     country_grouped = df_gr.groupby('location_country')

        #     return pd.Series({
        #         'views_by_country': country_grouped['views'].sum().to_dict(),
        #         'downloads_by_country': country_grouped['downloads'].sum().to_dict(),
        #         'outlinks_by_country': country_grouped['outlinks'].sum().to_dict(),
        #         'total_views': df_gr['views'].sum(),
        #         'total_downloads': df_gr['downloads'].sum(),
        #         'total_outlinks': df_gr['outlinks'].sum(),
        #     })


        # df_agg = merged_df.groupby('custom_var_v1').apply(custom_agg)
        # print(df_agg.sort_values(by='total_views', ascending=False))
        

        
        
        return data       
