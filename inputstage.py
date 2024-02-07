from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import pandas as pd
import awswrangler as wr


class InputStage(AbstractUsageStatsPipelineStage):
    
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)

        # get the s3 bucket from the configuration
        self.s3_bucket = configContext.getConfig('INPUT_STAGE', 'S3_BUCKET')
        self.visits_path = configContext.getConfig('INPUT_STAGE', 'VISITS_PATH')
        self.events_path = configContext.getConfig('INPUT_STAGE', 'EVENTS_PATH')

        # get the labels from the configuration
        self.COUNTRY_LABEL = configContext.getLabel('COUNTRY')

    def _partition_filter(idsite, year, month, day):
        
        idsite = str(idsite)
        year = str(year)
        month = str(month)
    
        if day is None:
            return lambda x: (x['idsite'] == idsite) and (x['year'] ==  year ) and (x['month'] == month)  
        else:
            day = str(day)
            return lambda x: (x['idsite'] == idsite) and (x['year'] ==  year ) and (x['month'] == month) and (x['day'] == day)
        
    
    def _read_parquet_file(bucket_path, columns, partition_filter):
            
        try: 
            df = wr.s3.read_parquet(
            path='s3://' + bucket_path,
            dataset=True,
            partition_filter = partition_filter,
            columns=columns
            )
            return df
        
        except:
            print("Error reading parquet file %s" % bucket_path)
            
    
    
    def run(self, data: UsageStatsData) -> UsageStatsData:

        year = self.getCtx().getArg('year')
        month = self.getCtx().getArg('month')
        day = self.getCtx().getArg('day')
        idsite = self.getCtx().getArg('site')

        partition_filter = InputStage._partition_filter(idsite, year, month, day)

        # read the events file       
        data.events_df  = InputStage._read_parquet_file( self.s3_bucket + self.events_path, 
            ['idlink_va', 'idvisit','server_time', 'custom_var_v1', 'custom_var_v3', 'action_type', 'action_url', 'action_url_prefix'],
            partition_filter )
        
        # rename the custom_var_v1 column to oai_identifier
        data.events_df = data.events_df.rename(columns={ 'custom_var_v1':'oai_identifier' })

        # read the visits file
        data.visits_df = InputStage._read_parquet_file ( self.s3_bucket + self.visits_path, 
            ['idvisit', 'visit_last_action_time', 'visit_first_action_time', 'visit_total_actions', 'location_country'],
            partition_filter )
        
        # rename the location_country column to country
        data.visits_df = data.visits_df.rename(columns={'location_country': self.COUNTRY_LABEL})
        
        return data
  


     