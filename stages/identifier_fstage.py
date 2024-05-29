from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import uuid


class IdentifierFilterStage(AbstractUsageStatsPipelineStage):

    IDENTIFIER_MAP_NORMALIZE = 0
    IDENTIFIER_MAP_REGEX_REPLACE = 1
    IDENTIFIER_MAP_FROM_FILE = 2

    
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)

        
    def run(self, data: UsageStatsData) -> UsageStatsData:

        identifier_map_regex = data.source.identifier_map_regex
        identifier_map_replace = data.source.identifier_map_replace
        identifier_map_filename = data.source.identifier_map_filename
        identifier_map_type = data.source.identifier_map_type
        identifier_prefix = data.source.identifier_prefix

    
        if data.source.identifier_map_type == IdentifierFilterStage.IDENTIFIER_MAP_REGEX_REPLACE:

            data.agg_dict['identifier'] = data.agg_dict['identifier'].apply(lambda x: self._regex_replace(x, data.source.identifier_map))


        ### using a map from a file to translate every identifier
        elif data.source.identifier_map_type == IdentifierFilterStage.IDENTIFIER_MAP_FROM_FILE:

            if data.source.identifier_map_file is None or data.source.identifier_map_file.strip() == '':
                raise ValueError("Identifier map file is not defined in the database source")
            
            try: 
                dict_to_search = {}        
                with open('identifier-dict.txt', 'r') as file:
                    for line in file:
                        key, value = line.split()
                        dict_to_search[key] = value
            except:
                raise ValueError("Error reading identifier map file %s" % data.source.identifier_map_file)    

       
        
        for key in list(data.agg_dict.keys()):
            if key in dict_to_search:
                data.agg_dict[dict_to_search[key]] = data.agg_dict.pop(key)

        return data       


