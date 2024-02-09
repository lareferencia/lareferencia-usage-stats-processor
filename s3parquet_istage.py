from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import pandas as pd
import awswrangler as wr


class S3ParquetInputStage(AbstractUsageStatsPipelineStage):
    
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)

        # get the s3 bucket and type of parquet from the configuration
        self.s3_bucket = configContext.getConfig('INPUT', 'S3_BUCKET')
        self.visits_path = configContext.getConfig('INPUT', 'VISITS_PATH')
        self.events_path = configContext.getConfig('INPUT', 'EVENTS_PATH')

        # get the labels from the configuration
        self.COUNTRY_LABEL = configContext.getLabel('COUNTRY')
        self.OAI_IDENTIFIER_LABEL = configContext.getLabel('OAI_IDENTIFIER')

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
        type = self.getCtx().getArg('type')

        partition_filter = S3ParquetInputStage._partition_filter(idsite, year, month, day)

        # set the custom_var column name based on the type
        identifier_custom_var = 'custom_var_v1' if type == 'R' else 'custom_var_v6'
        print(identifier_custom_var)
        print(type)

        # read the events file       
        data.events_df  = S3ParquetInputStage._read_parquet_file( self.s3_bucket + self.events_path, 
            ['idlink_va', 'idvisit','server_time', identifier_custom_var, 'action_type', 'action_url', 'action_url_prefix'],
            partition_filter )
        
        # rename the custom_var_v1 column to oai_identifier
        data.events_df = data.events_df.rename(columns={ identifier_custom_var: self.OAI_IDENTIFIER_LABEL })

        # read the visits file
        data.visits_df = S3ParquetInputStage._read_parquet_file ( self.s3_bucket + self.visits_path, 
            ['idvisit', 'visit_last_action_time', 'visit_first_action_time', 'visit_total_actions', 'location_country'],
            partition_filter )
        
        # rename the location_country column to country
        data.visits_df = data.visits_df.rename(columns={'location_country': self.COUNTRY_LABEL})
        
        return data
  


     