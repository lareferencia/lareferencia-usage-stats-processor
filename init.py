from processorpipeline import UsageStatsProcessorPipeline
from configcontext import ConfigurationContext

args = type('args', (object,), {})()
args.config_file = "config.tst.ini"

pipeline = UsageStatsProcessorPipeline( ConfigurationContext(args), 
                                       "inputstage.InputStage", 
                                       ["robotsfilterstage.RobotsFilterStage",
                                        "eventsfilterstage.EventsFilterStage"], 
                                       "outputstage.OutputStage" ) 


pipeline.run()
