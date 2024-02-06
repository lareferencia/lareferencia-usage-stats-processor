from processorpipeline import UsageStatsProcessorPipeline
from configcontext import ConfigurationContext
import time
import argparse
import datetime

args = type('args', (object,), {})()
args.config_file = "config.tst.ini"

start_time = time.time()



def main(args):
   
    
    # config_file_path = args.get("config_file_path", None)
    # site = args.get("site", None)
    # year = args.get("year", None)
    # month = args.get("month", None)
    # day = args.get("day", None)
    # date = args.get("date", None)
    
    try:
        pipeline = UsageStatsProcessorPipeline(ConfigurationContext(args), 
                                       "inputstage.InputStage", 
                                       [ "robotsfilterstage.RobotsFilterStage",
                                        "downloadeventsfilterstage.DownloadEventsFilterStage",
                                        "groupbyitemidvisitstage.GroupByItemIdvisitStage",
                                        "joineventsvisitsstage.JoinEventsVisitsStage",
                                        "groupbyitem.GroupByItem" ],
                                        "outputstage.OutputStage")
        pipeline.run()
        
    except Exception as e:
        print("Error reading config file: %s" % e)

    
    

def parse_args():
    parser = argparse.ArgumentParser(description="Usage Stats Processor", usage="python3 init.py -s <site> -y <year> --from_month <month> --to_month <month> --from_day <day> --to_day <day>" )
    
    #cambiar config.tst.ini por config.ini luego
    parser.add_argument( "-c", "--config_file_path", default='config.tst.ini', help="config file", required=False )
    parser.add_argument( "-s", "--site", default='all', help="site id", required=False)
    parser.add_argument( "-y", "--year", default=datetime.datetime.now().year, type=int, help="year", required=False )
    parser.add_argument("-month", default=1, type=int, help="from month", required=False)
    parser.add_argument("-day", default=None, type=int, help="to day", required=False)
    parser.add_argument("--date", type=str, default=None, help="date to process", required=False)   

    args = parser.parse_args()
    return args
    

if __name__ == "__main__":
    args = vars(parse_args())
    
    date = args.get("date")
    if date != None:
        try:
            datetime.datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYY-MM-DD")
        
    main(args)

end_time = time.time()

elapsed_time = end_time - start_time
print(f"Tiempo de ejecución: {elapsed_time} segundos")
