from processorpipeline import UsageStatsProcessorPipeline
from configcontext import ConfigurationContext
import time
import argparse
import datetime





def main(args):
   
    
    # config_file_path = args.get("config_file_path", None)
    # site = args.get("site", None)
    # year = args.get("year", None)
    # month = args.get("month", None)
    # day = args.get("day", None)
    # date = args.get("date", None)

    config_context = ConfigurationContext(args)
    
    try:
        pipeline = UsageStatsProcessorPipeline(config_context, 
                                       "inputstage.InputStage", 
                                       [ "robotsfilterstage.RobotsFilterStage",
                                        "downloadeventsfilterstage.DownloadEventsFilterStage",
                                        "groupbyitemidvisitstage.GroupByItemIdvisitStage",
                                        "joineventsvisitsstage.JoinEventsVisitsStage",
                                        "groupbyitem.GroupByItem" ],
                                        "outputstage.OutputStage")
        pipeline.run()
        
    except Exception as e:
        print("Error: %s" % e)
        # print traceback of e
        

    
    

def parse_args():
    parser = argparse.ArgumentParser(description="Usage Stats Processor", usage="python3 init.py -s <site> -y <year> --from_month <month> --to_month <month> --from_day <day> --to_day <day>" )
    
    #cambiar config.tst.ini por config.ini luego
    parser.add_argument( "-c", "--config_file_path", default='config.tst.ini', help="config file", required=False )
    parser.add_argument( "-s", "--site", default=48, help="site id", required=False)
   
    #parser.add_argument( "-y", "--year", default=datetime.datetime.now().year, type=int, help="year", required=False )
    parser.add_argument( "-y", "--year", default=2023, type=int, help="year", required=False )
    parser.add_argument("-m", "--month", default=1, type=int, help="month", required=False)
    parser.add_argument("-d", "--day", default=None, type=int, help="day", required=False)
   
    args = parser.parse_args()
    return args
    

if __name__ == "__main__":

    start_time = time.time()

    # parse arguments
    args = vars(parse_args())     
    
    # run the main function
    main(args)

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Tiempo de ejecución: {elapsed_time} segundos")
