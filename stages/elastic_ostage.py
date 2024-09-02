from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import awswrangler as wr
import sys
import datetime
import xxhash

class ElasticOutputStage(AbstractUsageStatsPipelineStage):

    MAPPING = {
        "properties" : {

            "id" : { "type" : "keyword" },
            "idsite" : { "type" : "long" },
            
            "date" : { "type" : "date" },
            
            "year" : { "type" : "long" },
            "month" : { "type" : "long" },
            "day" : { "type" : "long" },

            "level" : { "type" : "keyword" },
    
            "identifier" : { "type" : "text" },
            "country" : { "type" : "keyword" },
        }
    }

    def _build_stats(self, obj, stats):
        obj.update([(action, stats[action]) for action in self.actions])
        return obj


    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)

        # get the actions from the configuration
        self.actions = configContext.getActions()

        self.elastic_url = configContext.getConfig('OUTPUT','ELASTIC_URL')
        self.index_prefix = configContext.getConfig('OUTPUT','INDEX_PREFIX')

        self.COUNTRY_LABEL = configContext.getLabel('COUNTRY')
        self.STATS_BY_COUNTRY_LABEL = configContext.getLabel('STATS_BY_COUNTRY')

        self.level = configContext.getArg('type')
    
        # create the properties dict for the stats by country  
        self.MAPPING['properties'][self.STATS_BY_COUNTRY_LABEL] = { "type": "nested" }
        self.MAPPING['properties'][self.STATS_BY_COUNTRY_LABEL]['properties'] = {}

        # add the country label (2 letter)  to the stats by country properties
        self.MAPPING['properties'][self.STATS_BY_COUNTRY_LABEL]['properties'][self.COUNTRY_LABEL] = { "type" : "keyword" }
        
        # for each action, add a property (1 letter) to the MAPPING dictionary at the root level and to the stats by country
        for action in self.actions:
            self.MAPPING['properties'][action] = { "type" : "long" }
            self.MAPPING['properties'][self.STATS_BY_COUNTRY_LABEL]['properties'][action] = { "type" : "long" }

        self.helper = configContext.getDBHelper()


    def run(self, data: UsageStatsData) -> UsageStatsData:

        helper = self.getCtx().getDBHelper()

        year  = self.getCtx().getArg('year')
        month = self.getCtx().getArg('month')
        day   = self.getCtx().getArg('day')
        
        if day is None:
            day = 1

        idsite = self.getCtx().getArg('site')
        
        # calculate the country
        if self.level == 'L':
            pass # the country will be calculated for each identifier



        elif self.level == 'R':
            country = data.source.country_iso
        elif self.level == 'N':
            country = data.source.country_iso
        else:
            raise ValueError("Invalid level %s" % self.level)

        # create the index name
        index_name = helper.get_index_name(self.index_prefix, idsite, year)

         # transform dict_df into a list of documents, converting the dictionary into a list of documents and          
        data.documents = [
            self._build_stats( 
            {
              'id': xxhash.xxh64( '%s-%s-%s-%s-%s' % (idsite, identifier, year, month, day)  ).hexdigest(),
  
              'identifier': identifier, 

              self.STATS_BY_COUNTRY_LABEL: [ self._build_stats({ self.COUNTRY_LABEL: country }, country_data)
                                       for country, country_data in data[ self.STATS_BY_COUNTRY_LABEL ].items() ], 
                                   
              'date': datetime.datetime(year, month, day),
              
              'idsite': idsite,
              'year': year,
              'month': month,
              'day': day,
              'level': self.level,
              'country': self.helper.get_country_by_level_and_identifier(data.source, identifier)


            }, data)
            for identifier, data in data.agg_dict.items()
        ]

        try:
            opensearch = wr.opensearch.connect(
                host=self.elastic_url
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
                 settings={
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    }
                },
                index=index_name,  )
            print ('Index %s created' % (index_name))
        except:
            print ('Index %s already exists' % (index_name))


        wr.opensearch.index_documents(
            client=opensearch,
            index=index_name,
            documents=data.documents,
            id_keys=["id"],
            bulk_size=10000
        )
                
        return data