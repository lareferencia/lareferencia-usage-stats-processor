from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext


class InputStage(AbstractUsageStatsPipelineStage):
    
    def __init___(self, configContext: ConfigurationContext):
        super().__init__(configContext)
    
    def run(self, data: UsageStatsData) -> UsageStatsData:
        data.input_stage = "Data frame ingresado"
        return data