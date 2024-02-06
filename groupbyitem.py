from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import copy
import uuid
import datetime

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
                
        
        tmp_dict = {}
        
        for index, row in data.events_df.iterrows():
            
            identifier = row['oai_identifier']
            entry = tmp_dict.get(identifier, copy.deepcopy(empty_entry) )    
            tmp_dict[identifier] = entry
            country = row['location_country']
            
            for action in actions:
                if row[action] > 0:
                    entry[action][country] = entry[action].get(country, 0) + row[action]
                    entry['total_' + action] += row[action]
                
                           
        # transform dict_df into a list of documents         
        data.documents = [
            {**data, 'identifier': identifier, 'id': uuid.uuid4(), 'date': datetime.datetime.now()}
            for identifier, data in tmp_dict.items()
        ]

        # delete the reference to the temporary dictionary
        del tmp_dict
    
        return data       
