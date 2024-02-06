from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import copy
import uuid
import datetime

class GroupByItem(AbstractUsageStatsPipelineStage):

    STATS_BY_COUNTRY = 'stats_by_country'

    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)

        # get the list of actions from the configuration
        self.actions = self._configContext._config['JOIN_EVENTS_VISITS_STAGE']['ACTIONS_LIST']
        self.actions = self.actions.split(', ')
        
        # create an empty entry for the dictionary
        self.empty_entry = { "views": 0, "downloads": 0, "outlinks": 0, "conversions": 0 }

    def run(self, data: UsageStatsData) -> UsageStatsData:
            
        # create a temporary dictionary to store the data
        tmp_dict = {}
        
        # iterate over the rows of the dataframe
        for index, row in data.events_df.iterrows():
            
            # get the entry for the identifier or create a new one
            identifier = row['oai_identifier']
            entry = tmp_dict.get(identifier, copy.deepcopy(self.empty_entry) ) 
            tmp_dict[identifier] = entry

            # if the entry does not have the stats by country, create a new one
            if entry.get(self.STATS_BY_COUNTRY) is None:
                entry[self.STATS_BY_COUNTRY] = {}

            # get the country and the entry for the country or create a new one
            country = row['location_country']
            country_entry = entry[ self.STATS_BY_COUNTRY ].get(country, copy.deepcopy(self.empty_entry))
            entry[ self.STATS_BY_COUNTRY ][country] = country_entry
            
            for action in self.actions:
                if row[action] > 0:
                    entry[action] += 1
                    country_entry[action] += 1
                           
        # transform dict_df into a list of documents, converting the dictionary into a list of documents and          
        data.documents = [
            { 'identifier': identifier, 
              'id': identifier, 
              'views': data['views'],
              'downloads': data['downloads'],
              'outlinks': data['outlinks'],
              'conversions': data['conversions'],
              'stats_by_country': [ {
                                   'country': country, 
                                   'views': country_data['views'], 
                                   'downloads': country_data['downloads'], 
                                   'outlinks': country_data['outlinks'], 
                                   'conversions': country_data['conversions']
                                   } 
                                   for country, country_data in data[ self.STATS_BY_COUNTRY ].items() ], 
                                   
              'date': datetime.datetime.now()
            }
            for identifier, data in tmp_dict.items()
        ]

        # delete the reference to the temporary dictionary
        del tmp_dict
    
        return data       
