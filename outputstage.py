from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import awswrangler as wr
import sys


class OutputStage(AbstractUsageStatsPipelineStage):

    MAPPING = {
        "properties" : {

            "id" : { "type" : "keyword" },
            
            "date" : { "type" : "date" },
            
            "year" : { "type" : "long" },
            "month" : { "type" : "long" },
            "day" : { "type" : "long" },
    
            "identifier" : { "type" : "text" },
            "identifier_prefix" : { "type" : "text" },

            "views" :       { "type" : "long" },
            "downloads" :   { "type" : "long" },
            "conversions" : { "type" : "long" },
            "outlinks" :    { "type" : "long"},

            "stats_by_country" : {
                "properties" : {
                    "country" :     { "type" : "keyword" },
                    "views" :       { "type" : "long" },
                    "downloads" :   { "type" : "long" },
                    "conversions" : { "type" : "long" },
                    "outlinks" :    { "type" : "long"}        
                }
            }
        }
    }

    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:

        elastic_url = self._configContext._config['OUTPUT_STAGE']['ELASTIC_URL']
        index_name = self._configContext._config['OUTPUT_STAGE']['INDEX_NAME']

        try:
            opensearch = wr.opensearch.connect(
                host=elastic_url
        #     username='FGAC-USERNAME(OPTIONAL)',
        #     password='FGAC-PASSWORD(OPTIONAL)'
        )
        except Exception as e:
            print("Error connecting to opensearch: %s" % e)
            sys.exit(1)

        try:
            index = wr.opensearch.create_index(
                client=opensearch,
                mappings=self.MAPPING,
                index=index_name )
            print ('Index %s created' % (index_name))
        except:
            print ('Index %s already exists' % (index_name))


        wr.opensearch.index_documents(
            client=opensearch,
            index=index_name,
            documents=data.documents,
            id_keys=["id"],
            bulk_size=1000
        )
    
        return data