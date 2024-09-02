from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext



class ByIdentifierOutputStage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
        
    
    def run(self, data: UsageStatsData) -> UsageStatsData:

        idsite = self.getCtx().getArg('site')
        year = self.getCtx().getArg('year')
        month = self.getCtx().getArg('month')
        identifier = self.getCtx().getArg('identifier')

        stats =  data.agg_dict.get(identifier) 

        if stats is None:
            print("No stats for identifier %s" % identifier)
            return None

        ## print views, downloads, conversins and outlinks from dictionary
        print("Identifier: %s" % identifier)
        print("Views: %d" % stats['views'])
        print("Downloads: %d" % stats['downloads'])
        print("Conversions: %d" % stats['conversions'])
        print("Outlinks: %d" % stats['outlinks'])


        return data
     
 
        