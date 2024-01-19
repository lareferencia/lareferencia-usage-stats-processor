from processorpipeline import UsageStatsProcessorPipeline
from configcontext import ConfigurationContext
import time

args = type('args', (object,), {})()
args.config_file = "config.tst.ini"

start_time = time.time()

pipeline = UsageStatsProcessorPipeline(ConfigurationContext(args), 
                                       "inputstage.InputStage", 
                                       ["groupbyidvisit.GroupByIdVisit", "robotsfilterstage.RobotsFilterStage"],
                                       "outputstage.OutputStage")

pipeline.run()

end_time = time.time()

elapsed_time = end_time - start_time
print(f"Tiempo de ejecución: {elapsed_time} segundos")
