from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext


class InputStage(AbstractUsageStatsPipelineStage):
    
    def __init___(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
        data = UsageStatsData()
        data.input_stage = "Input Stage holi"
        print(self._configContext)
        return data
    

args = type('args', (object,), {})()
args.config_file = "config.tst.ini"
        
config_context = ConfigurationContext(args)
input_stage = InputStage(config_context)
print(input_stage.run(UsageStatsData()))