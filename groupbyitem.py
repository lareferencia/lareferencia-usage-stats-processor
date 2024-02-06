from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import copy


class GroupByItem(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
        
        empty_entry = {}
        
        actions = self._configContext._config['JOIN_EVENTS_VISITS_STAGE']['ACTIONS_LIST']
        actions = actions.split(', ')
        
        for action in actions:
            empty_entry[action] = {}
            empty_entry['total_' + action] = 0
                
        
        dict_df = {}
        
        for index, row in data.events_df.iterrows():
            
            identifier = row['oai_identifier']
            entry = dict_df.get(identifier, copy.deepcopy(empty_entry) )    
            dict_df[identifier] = entry
            country = row['location_country']
            
            for action in actions:
                if row[action] > 0:
                    entry[action][country] = entry[action].get(country, 0) + row[action]
                    entry['total_' + action] += row[action]
                
            
        # for identifier, entry in dict_df.items():
        #         print( '%s %s' % (identifier, entry) )
                
        
    
        return data       
