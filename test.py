from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsProcessorPipeline, UsageStatsData
from configcontext import ConfigurationContext

class TestInputStage(AbstractUsageStatsPipelineStage):

    def run(self, data: UsageStatsData) -> UsageStatsData:
        data = UsageStatsData()
        data.test_input = "Test Input Stage"
        return data
    
class TestOutputStage(AbstractUsageStatsPipelineStage):

    def run(self, data: UsageStatsData) -> UsageStatsData:
        data.test_output = "Test Output Stage"
        return data

class TestFilterStage(AbstractUsageStatsPipelineStage):

    def run(self, data: UsageStatsData) -> UsageStatsData:
        data.test_filter = "Test Filter Stage"
        return data
    
import unittest

class TestUsageStatsProcessorPipeline(unittest.TestCase):

    def setUp(self):

        args = type('args', (object,), {})()
        args.config_file = "config.tst.ini"

        self.configContext = ConfigurationContext(args)
        self.pipeline = UsageStatsProcessorPipeline( TestInputStage(self.configContext), 
                                                    [TestFilterStage(self.configContext)], 
                                                     TestOutputStage(self.configContext) )

    def test_pipeline(self):
        # Agregar etapas al pipeline
        output_data = self.pipeline.run()

        # Verificar los resultados
        self.assertEqual(output_data.test_input, "Test Input Stage")
        self.assertEqual(output_data.test_filter, "Test Filter Stage")
        self.assertEqual(output_data.test_output, "Test Output Stage")

if __name__ == '__main__':
    unittest.main()