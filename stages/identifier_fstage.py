from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import uuid
import re
from lareferenciastatsdb import normalize_oai_identifier

class IdentifierFilterStage(AbstractUsageStatsPipelineStage):

    IDENTIFIER_MAP_NORMALIZE = 0
    IDENTIFIER_MAP_REGEX_REPLACE = 1
    IDENTIFIER_MAP_FROM_FILE = 2

    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
        #self.dbhelper = configContext.getDBHelper()

        
    def run(self, data: UsageStatsData) -> UsageStatsData:

        identifier_map_regex = data.source.identifier_map_regex
        identifier_map_replace = data.source.identifier_map_replace
        identifier_map_filename = data.source.identifier_map_filename
        identifier_map_type = data.source.identifier_map_type
        identifier_prefix = data.source.identifier_prefix

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
                        #print(line)
                        key, value = line.split(',')
                        dict_to_search[key] = value
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

        # for every identifier in the data
        for old_identifier in list(data.agg_dict.keys()):

            hits = 0
            # if the identifier map type is map from file, get the new identifier from the dictionary
            if identifier_map_type == IdentifierFilterStage.IDENTIFIER_MAP_FROM_FILE:
                if old_identifier in dict_to_search:
                    new_identifier = dict_to_search[old_identifier]
                    hits += 1

            # normalize the identifier
            new_identifier = normalize_oai_identifier(old_identifier)
            
            # if the identifier map type is regex replace, apply the regex
            if identifier_map_type == IdentifierFilterStage.IDENTIFIER_MAP_REGEX_REPLACE:
                new_identifier = regex.sub(identifier_map_replace, old_identifier)

            #print(old_identifier, " --> " ,new_identifier)

            # if the identifier has changed, update the dictionary
            if new_identifier != old_identifier:
                data.agg_dict[new_identifier] = data.agg_dict.pop(old_identifier)
                ##file.write(old_identifier + " --> " + str(new_identifier) + "\n")

        if identifier_map_type == IdentifierFilterStage.IDENTIFIER_MAP_FROM_FILE:
            print("Hits in map:", hits)
                
        print("Normalized identifiers:", len(data.agg_dict.keys()))


        return data       


