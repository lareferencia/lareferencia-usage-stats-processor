from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import re
from lareferenciastatsdb import normalize_oai_identifier

class IdentifierFilterStage(AbstractUsageStatsPipelineStage):

    IDENTIFIER_MAP_NORMALIZE = 0
    IDENTIFIER_MAP_REGEX_REPLACE = 1
    IDENTIFIER_MAP_FROM_FILE = 2

    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
        self.actions = configContext.getActions()
        self.STATS_BY_COUNTRY_LABEL = configContext.getLabel('STATS_BY_COUNTRY')

    def _merge_info(self, target_info, source_info):
        for action in self.actions:
            target_info[action] = target_info.get(action, 0) + source_info.get(action, 0)

        target_by_country = target_info.setdefault(self.STATS_BY_COUNTRY_LABEL, {})
        for country, source_country_info in source_info.get(self.STATS_BY_COUNTRY_LABEL, {}).items():
            target_country_info = target_by_country.setdefault(country, {})
            for action in self.actions:
                target_country_info[action] = target_country_info.get(action, 0) + source_country_info.get(action, 0)

        
    def run(self, data: UsageStatsData) -> UsageStatsData:

        identifier_map_regex = data.source.identifier_map_regex
        identifier_map_replace = data.source.identifier_map_replace
        identifier_map_filename = data.source.identifier_map_filename
        identifier_map_type = data.source.identifier_map_type

        ## write a file with the changed identifiers
        ##file = open("identifiers.txt", "a") 
        
        # if the identifier map type is map from file, read the file
        if identifier_map_type == IdentifierFilterStage.IDENTIFIER_MAP_FROM_FILE:
            if identifier_map_filename is None or identifier_map_filename.strip() == '':
                raise ValueError("Identifier map file is not defined in the database source")
            
            # print loading message
            print("Loading identifier map from file %s" % identifier_map_filename)

            try: 
                dict_to_search = {}        
                with open(identifier_map_filename, 'r') as file:
                    for line in file:
                        line = line.strip()
                        if not line:
                            continue
                        key, value = line.split(',', 1)
                        dict_to_search[key.strip()] = value.strip()
            except:
                raise ValueError("Error reading identifier map file %s" % identifier_map_filename)
            
            # print the number of identifiers in the map
            print("Identifiers in map:", len(dict_to_search.keys()))
            
        # if the identifier map type is regex replace, compile the regex
        elif identifier_map_type == IdentifierFilterStage.IDENTIFIER_MAP_REGEX_REPLACE:
            try:
                regex = re.compile(identifier_map_regex)
            except:
                raise ValueError("Invalid regex %s" % identifier_map_regex)

        print("Identifiers:", len(data.agg_dict.keys()))

        hits = 0
        normalized_agg_dict = {}
        normalized_country_by_identifier_dict = {}
        # for every identifier in the data
        for old_identifier, old_info in data.agg_dict.items():
            new_identifier = old_identifier

            # if the identifier map type is map from file, get the new identifier from the dictionary
            if identifier_map_type == IdentifierFilterStage.IDENTIFIER_MAP_FROM_FILE:
                if old_identifier in dict_to_search:
                    new_identifier = dict_to_search[old_identifier]
                    hits += 1

            # if the identifier map type is regex replace, apply the regex
            elif identifier_map_type == IdentifierFilterStage.IDENTIFIER_MAP_REGEX_REPLACE:
                new_identifier = regex.sub(identifier_map_replace, old_identifier)
            else:
                new_identifier = normalize_oai_identifier(old_identifier)

            #print(old_identifier, " --> " ,new_identifier)

            if new_identifier in normalized_agg_dict:
                self._merge_info(normalized_agg_dict[new_identifier], old_info)
            else:
                normalized_agg_dict[new_identifier] = old_info

            old_country = data.country_by_identifier_dict.get(old_identifier)
            if old_country is not None and new_identifier not in normalized_country_by_identifier_dict:
                normalized_country_by_identifier_dict[new_identifier] = old_country

        data.agg_dict = normalized_agg_dict
        data.country_by_identifier_dict = normalized_country_by_identifier_dict

        if identifier_map_type == IdentifierFilterStage.IDENTIFIER_MAP_FROM_FILE:
            print("Hits in map:", hits)
                
        print("Normalized identifiers:", len(data.agg_dict.keys()))


        return data       

