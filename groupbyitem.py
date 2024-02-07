from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import copy
import uuid
import datetime

class GroupByItem(AbstractUsageStatsPipelineStage):


    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
     
        # get the actions from the configuration
        self.actions = configContext.getActions()

        # create an empty entry for the dictionary
        self.empty_entry = {}
        for action in self.actions:
            self.empty_entry[action] = 0

        # get the labels from the configuration
        self.COUNTRY_LABEL = configContext.getLabel('COUNTRY')
        self.STATS_BY_COUNTRY_LABEL = configContext.getLabel('STATS_BY_COUNTRY')
        self.OAI_IDENTIFIER_LABEL = configContext.getLabel('OAI_IDENTIFIER')

    def run(self, data: UsageStatsData) -> UsageStatsData:


        # create a temporary dictionary to store the data
        data.agg_dict = {}
        
        # iterate over the rows of the dataframe
        for index, row in data.events_df.iterrows():
            
            # get the entry for the identifier or create a new one
            identifier = row[ self.OAI_IDENTIFIER_LABEL ]
            entry = data.agg_dict.get(identifier, copy.deepcopy(self.empty_entry) ) 
            data.agg_dict[identifier] = entry

            # if the entry does not have the stats by country, create a new one
            if entry.get(self.STATS_BY_COUNTRY_LABEL) is None:
                entry[self.STATS_BY_COUNTRY_LABEL] = {}

            # get the country and the entry for the country or create a new one
            country = row[ self.COUNTRY_LABEL ]
            country_entry = entry[ self.STATS_BY_COUNTRY_LABEL ].get(country, copy.deepcopy(self.empty_entry))
            entry[ self.STATS_BY_COUNTRY_LABEL ][country] = country_entry
            
            for action in self.actions:
                if row[action] > 0:
                    entry[action] += 1
                    country_entry[action] += 1
                           
           
        return data       
