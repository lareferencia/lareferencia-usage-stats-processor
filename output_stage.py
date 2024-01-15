from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext


class Input_stage(AbstractUsageStatsPipelineStage):
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
        data = UsageStatsData()
        data.input_stage = "Output Stage holi"
        return data
    
    
args = type('args', (object,), {})()
args.config_file = "config.tst.ini"
config_context = ConfigurationContext(args)
input_stage = Input_stage(config_context)
print(input_stage.run(UsageStatsData()))
