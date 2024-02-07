from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import copy
import uuid
import datetime

class GroupByItem(AbstractUsageStatsPipelineStage):

    STATS_BY_COUNTRY = 'stats_by_country'
    OAI_IDENTIFIER = 'oai_identifier'
    COUNTRY = 'location_country'

    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)

        # get the list of actions from the configuration
        self.actions = self._configContext._config['JOIN_EVENTS_VISITS_STAGE']['ACTIONS_LIST']
        self.actions = self.actions.split(', ')
        
        # create an empty entry for the dictionary
        self.empty_entry = {}
        for action in self.actions:
            self.empty_entry[action] = 0

    def _build_stats(self, obj, stats):
        obj.update([(action, stats[action]) for action in self.actions])
        return obj

    def run(self, data: UsageStatsData) -> UsageStatsData:

        year = self.getArgs()['year']
        month = self.getArgs()['month']
        
        day = self.getArgs()['day']
        if day is None:
            day = 1
        
        idsite = self.getArgs()['site']
            
        # create a temporary dictionary to store the data
        tmp_dict = {}
        
        # iterate over the rows of the dataframe
        for index, row in data.events_df.iterrows():
            
            # get the entry for the identifier or create a new one
            identifier = row[ self.OAI_IDENTIFIER ]
            entry = tmp_dict.get(identifier, copy.deepcopy(self.empty_entry) ) 
            tmp_dict[identifier] = entry

            # if the entry does not have the stats by country, create a new one
            if entry.get(self.STATS_BY_COUNTRY) is None:
                entry[self.STATS_BY_COUNTRY] = {}

            # get the country and the entry for the country or create a new one
            country = row[ self.COUNTRY ]
            country_entry = entry[ self.STATS_BY_COUNTRY ].get(country, copy.deepcopy(self.empty_entry))
            entry[ self.STATS_BY_COUNTRY ][country] = country_entry
            
            for action in self.actions:
                if row[action] > 0:
                    entry[action] += 1
                    country_entry[action] += 1
                           
        # transform dict_df into a list of documents, converting the dictionary into a list of documents and          
        data.documents = [
            self._build_stats( 
            {
              'id': '%s-%s-%s-%s' % (identifier, year, month, day),
  
              'identifier': identifier, 

              self.STATS_BY_COUNTRY: [ self._build_stats({ 'country': country }, country_data)
                                       for country, country_data in data[ self.STATS_BY_COUNTRY ].items() ], 
                                   
              'date': datetime.datetime(year, month, day),
              
              'idsite': idsite,
              'year': year,
              'month': month,
              'day': day,

            }, data)
            for identifier, data in tmp_dict.items()
        ]

        # delete the reference to the temporary dictionary
        del tmp_dict
    
        return data       
