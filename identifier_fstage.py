from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import uuid

class IdentifierFilterStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)

        
    def run(self, data: UsageStatsData) -> UsageStatsData:
        
    
        
        dict_to_search = {}
        
        with open('identifier-dict.txt', 'r') as file:
            for line in file:
                key, value = line.split()
                dict_to_search[key] = value
        
        for key in list(data.agg_dict.keys()):
            
            if key in dict_to_search:
                data.agg_dict[dict_to_search[key]] = data.agg_dict.pop(key)

        return data       


