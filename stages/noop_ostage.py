from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext


class NoOpOutputStage(AbstractUsageStatsPipelineStage):

    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)

    def run(self, data: UsageStatsData) -> UsageStatsData:
        print("Dry run mode: skipping OpenSearch indexing")
        return data
