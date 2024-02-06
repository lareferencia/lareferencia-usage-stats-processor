from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import pandas as pd
import awswrangler as wr


class InputStage(AbstractUsageStatsPipelineStage):
    
    def __init___(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
    
        s3_bucket = self.getConfig()['S3']['BUCKET']
        
        def partition_filter(idsite, year, month, day):
            
            idsite = str(idsite)
            year = str(year)
            month = str(month)
            day = str(day)
            
            
            return lambda x: (x['idsite'] == idsite) and (x['year'] ==  year ) and (x['month'] == month) and (x['day'] == day)
        
        
        def read_parquet_file(type, columns):
            
            year = self.getArgs()["year"]
            print(year)
            
            
            try: 
                df = wr.s3.read_parquet(
                path='s3://' + s3_bucket + type,
                dataset=True,
                partition_filter = partition_filter( 48, year, 1, 2 ),
                columns=columns
                )
                
                df = df.rename(columns={ 'custom_var_v1':'oai_identifier' })
                return df
            
            except:
                print('Error reading parquet file')
     
            
        data.events_df  = read_parquet_file ('/events', [
            'idlink_va', 'idvisit','server_time', 'custom_var_v1', 
            'custom_var_v3', 'action_type', 'action_url', 'action_url_prefix'
            ])
        
        data.visits_df = read_parquet_file ('/visits', [
            'idvisit', 'visit_last_action_time', 'visit_first_action_time', 
            'visit_total_actions', 'location_country'
            ])
        
        

        return data
  


     