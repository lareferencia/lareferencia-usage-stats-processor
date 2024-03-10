from processorpipeline import UsageStatsProcessorPipeline
from configcontext import ConfigurationContext
import time
import argparse
import datetime





def main(args):
   
    pass        
    

def parse_args():
    parser = argparse.ArgumentParser(description="Usage Stats Processor", usage="python3 init.py -s <site> -y <year> --from_month <month> --to_month <month> --from_day <day> --to_day <day>" )
    
    #cambiar config.tst.ini por config.ini luego
    parser.add_argument( "-c", "--config_file_path", default='config.tst.ini', help="config file", required=False )
    parser.add_argument( "-s", "--site", default=48, help="site id", required=False)
   
    parser.add_argument( "-t", "--type", default='R', type=str, help="(R|L|N)", required=False )

    parser.add_argument( "-y", "--year", default=2023, type=int, help="yyyy", required=False )
    parser.add_argument("-m", "--month", default=1, type=int, help="m", required=False)
    parser.add_argument("-d", "--day", default=None, type=int, help="d", required=False)
   
    args = parser.parse_args()
    return args
    

if __name__ == "__main__":

    start_time = time.time()

    # parse arguments
    args = vars(parse_args())
    print("Arguments: ", args )     
    
    # run the main function
    main(args)

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Tiempo de ejecución: {elapsed_time} segundos")
