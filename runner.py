import logging
import os
import sys
import argparse
from calendar import monthrange
import requests
import gc

import pandas as pd
import time

import datetime

from config import read_ini

import atexit
from s3logger import S3Logger 

from lareferenciastatsdb import UsageStatsDatabaseHelper

import subprocess


def process_site(command, configfile,  site_id, year, month, day, type):
    print("Processing %s site: %s year: %s month: %s day: %s" % (command, site_id, year, month, day))

     # Iniciar la lista de comandos con el comando en sí
    cmd_list = [command]

    if configfile is not None:
        cmd_list.append("--config_file_path=" + str(configfile))

    # Agregar parámetros solo si no son nulos
    if site_id is not None:
        cmd_list.append("--site=" + str(site_id))
    if year is not None:
        cmd_list.append("--year=" + str(year))
    if month is not None:
        cmd_list.append("--month=" + str(month))
    if day is not None:
        cmd_list.append("--day=" + str(day))
    if type is not None:
        cmd_list.append("--type=" + str(type))


    # Ejecutar el comando
    command = " ".join(cmd_list)    
    print("Executing command: %s" % " ".join(cmd_list))
    result = subprocess.run(command, capture_output=True, text=True, shell=True)

    # Imprimir el resultado
    print(result.stdout)
    print(result.stderr)

def main(args_dict):

    # get arguments
    config_file_path = args_dict.get('config_file_path', None)

    site = args_dict.get('site', None)
    
    year = args_dict.get('year', None) 
    
    from_month = args_dict.get('from_month', None) 
    to_month = args_dict.get('to_month', None) 
    
    from_day = args_dict.get('from_day', None)
    to_day = args_dict.get('to_day', None)
    
    exclude_sites = args_dict.get('exclude_sites', []) 
    
    from_site = args_dict.get('from_site', 0)
    to_site = args_dict.get('to_site', sys.maxsize)
    
    date = args_dict.get('date', None)

    command = args_dict.get('process', "echo")
    
    # logger for s3
    s3logger = S3Logger( command.replace(" ", "_") + "_logs")
    
    dry_run = args_dict.get('dry_run', False)

    try: 
        # read config file
        config = read_ini(config_file_path)

        # database helper
        dbhelper = UsageStatsDatabaseHelper(config)
        
        # logger bucket
        #s3logger.set_bucket(config["S3"]["LOGS_BUCKET"])

        #register write logs to s3 at exit
        #atexit.register(s3logger.write, name="end")

    except Exception as e:
        print("Error %s" % e)
        # print stack trace
        import traceback
        traceback.print_exc()
        sys.exit(1)


    # sites list 
    if site == 'all':
        sites = list(dbhelper.get_all_site_ids())
        sites.sort()

        # exclude sites
        for exclude_site in exclude_sites:
            sites.remove(exclude_site)

        sites = list(filter(lambda x: x >= from_site and x <= to_site, sites))

    else:
        sites = [int(site)]
           
    # # loop over sites
    for site_id in sites:

        source = dbhelper.get_source_by_site_id(site_id)


         # if date is specified, get only that day
        if ( date != None ):
            
            # get date from string
            year = int(date[0:4])
            month = int(date[5:7])
            day = int(date[8:10])

            process_site(command, config_file_path, site_id, year, month, day, source.type)

        else:
            # if from_month, to_month, from_day, to_day are None get all data for the year
            if (from_month is not None and to_month is not None and from_day is not None and to_day is not None):
                process_site(command, config_file_path, site_id, year, None, None, source.type)

            # if not specified date, get data for all days coverd by from_month and to_month
            else:

                # if from_month is not specified, process all months
                if from_month is None:
                    from_month = 1
                
                # if to_month is not specified, process all months
                if to_month is None:
                    to_month = 12

                # loop over months
                for month in range(from_month,to_month+1):

                    ## if from_day is not specified, process all month
                    if from_day is None:
                        process_site(command, config_file_path, site_id, year, month, None, source.type)

                    else: # process only from_day to to_day    

                        # if to_day is not specified and from_data is 1 process all month by passsing None as day
                        if to_day is None:
                            local_to_day = monthrange(year,month)[1]
                        else:
                            local_to_day = to_day

                        # loop over days
                        for day in range(from_day,local_to_day+1):
                            process_site(command, config_file_path, site_id, year, month, day, source.type)

             

        # log finish site
        s3logger.loginfo("Finished site: %s" % (site_id))

        # write logs of this site
        s3logger.write( str(site_id) )

        # garbage collection
        gc.collect()    

    # s3logger.loginfo("Ending procesing on datetime: %s site: %s year: %s from_month: %s to_month: %s from_day: %s to_day: %s" % ( datetime.datetime.now(), site, year, from_month, to_month, from_day, to_day))


def parse_args():

    parser = argparse.ArgumentParser(description="Usage Statistics Process Batch Runner", usage='python3 runner.py --process="<somecode.py>" -s <site> -y <year> --from_month <month> --to_month <month> --from_day <day> --to_day <day>')

    parser.add_argument("--loglevel", 
                        help="Log level", 
                        default=logging.INFO)
    
    parser.add_argument("-p", "--process", 
                        help="Process to run", 
                        required=True)

    parser.add_argument("-c",
                        "--config_file_path",
                        default='config.ini',
                        help="config file",
                        required=False)

    parser.add_argument("-s",
                        "--site",
                        default='all',
                        help="site id",
                        required=False)

    parser.add_argument("-y",
                        "--year",
                        default=datetime.datetime.now().year,
                        type=int,
                        help="year",
                        required=False)

    parser.add_argument("--from_month",
                        default=1,
                        type=int,
                        help="from month",
                        required=False)

    parser.add_argument("--to_month",
                        default=12,
                        type=int,
                        help="to month",
                        required=False)

    parser.add_argument("--from_day",
                        default=None,
                        type=int,   
                        help="from day",
                        required=False)

    parser.add_argument("--to_day",
                        default=None,
                        type=int,
                        help="to day",
                        required=False)

    parser.add_argument("--date", type=str, default=None, help="date to process", required=False)   

    parser.add_argument("-v", "--verbose", default=False, type=bool, help="verbose mode")

    parser.add_argument("--exclude_sites",type=int, default=[], help="exclude sites", nargs='+')

    parser.add_argument("--from_site",
                    default=0,
                    type=int,   
                    help="from site",
                    required=False)

    parser.add_argument("--to_site",
                    default=sys.maxsize,
                    type=int,   
                    help="to site",
                    required=False)

    parser.add_argument("--dry_run", default=False, type=bool, required=False, help="dont write to elastic")

    args = parser.parse_args()

    return args 

  

if __name__ == "__main__":

    args = vars(parse_args())

    # set log level
    # s3logger.set_log_level(args.get('loglevel'))
    
    # if (args.get('verbose')):
    #     s3logger.set_log_level(logging.INFO)
    #     s3logger.set_local_mode(True)

    # validate date format
    date = args.get('date')
    if date != None:
        try:
            datetime.datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYY-MM-DD")
        

    start_time = time.time()
                
    # call main function
    main(args)

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Tiempo de ejecución: {elapsed_time} segundos")