import logging
import os
import sys
import argparse
from calendar import monthrange
from unicodedata import name
import requests
import json
import gc

import pymysql
import awswrangler as wr
import pandas as pd

import datetime

from config import read_ini

import atexit
from s3logger import S3Logger 

# logger for s3
s3logger = S3Logger('matomo2parquet');

def main(args_dict):

    config_file_path = args_dict.get('config_file_path', None)

    site = args_dict.get('site', None)
    year = args_dict.get('year', None) 
    month = args_dict.get('month', None)
    day = args_dict.get('day', None)

    dry_run = args_dict.get('dry_run', False)

    try: 
        # read config file
        config = read_ini(config_file_path);

        db_host = config["MATOMO_DB"]["HOST"] 
        db_username = config["MATOMO_DB"]["USERNAME"] 
        db_passwd = config["MATOMO_DB"]["PASSWORD"] 
        db_database = config["MATOMO_DB"]["DATABASE"] 

        s3_visits_bucket = config["S3_STATS"]["VISITS_PATH"]
        s3_events_bucket = config["S3_STATS"]["EVENTS_PATH"]
   
    
        # logger bucket
        s3logger.set_bucket(config["S3_LOGS"]["LOGS_PATH"])
        s3logger.loginfo("Starting procesing on datetime: %s site: %s year: %s month: %s day: %s" % ( datetime.datetime.now(), site, year, month, day))

        # register write logs to s3 at exit
        #atexit.register(s3logger.write, name="end")

    except Exception as e:
        print("Error : %s" % e)
        # print stack trace
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # connect to mysql
    try:
        conn = pymysql.connect(host=db_host, user=db_username, passwd=db_passwd, db=db_database, connect_timeout=5)
    except pymysql.MySQLError as e:
        s3logger.logerror("ERROR: Unexpected error: Could not connect to MySQL instance.")
        s3logger.logerror(e)
        sys.exit()

    
    # get data from mysql
    # if day is not specified, throw error
    if day is None:
        #calculate the last day of the month
        last_day = monthrange(year, month)[1]
       
        visit_query = """SELECT * FROM matomo_log_visit WHERE idvisit in 
                        (SELECT idvisit FROM matomo_log_link_visit_action WHERE idsite = {3} and server_time BETWEEN '{0:04d}-{1:02d}-01 00:00:00.000' AND '{0:04d}-{1:02d}-{2:02d} 23:59:59.999')
                        """.format(year,month,last_day,site)
        
        event_query = """SELECT va.*, a.`type` as action_type, a.name  as action_url, a.url_prefix as action_url_prefix
                            FROM (matomo_log_link_visit_action va left join matomo_log_action a on (va.idaction_url = a.idaction)) 
                            WHERE idsite = {3} and server_time BETWEEN '{0:04d}-{1:02d}-01 00:00:00.000' AND '{0:04d}-{1:02d}-{2:02d} 23:59:59.999' AND NOT RIGHT(a.name,8) = '.pdf.jpg'
                        """.format(year,month,last_day,site) 
    else:
        visit_query = "SELECT * FROM matomo_log_visit WHERE idsite = {3} and visit_last_action_time BETWEEN '{0:04d}-{1:02d}-{2:02d} 00:00:00' AND '{0:04d}-{1:02d}-{2:02d} 23:59:59'".format(year,month,day,site)
        event_query = """SELECT va.*, a.`type` as action_type, a.name  as action_url, a.url_prefix as action_url_prefix
                            FROM (matomo_log_link_visit_action va left join matomo_log_action a on (va.idaction_url = a.idaction)) 
                            WHERE idsite = {3} and server_time BETWEEN '{0:04d}-{1:02d}-{2:02d} 00:00:00.000' AND '{0:04d}-{1:02d}-{2:02d} 23:59:59.999' AND NOT RIGHT(a.name,8) = '.pdf.jpg'
                        """.format(year,month,day,site)

    
    # read visit data
    #print(visit_query)
    visit_df = wr.mysql.read_sql_query(sql=visit_query,con=conn)
    
    #print(event_query)
    event_df = wr.mysql.read_sql_query(sql=event_query,con=conn)

    if visit_df.empty:
        s3logger.logwarning("No data for site: %s year: %s month: %s day: %s" % (site, year, month, day))
    else:
        # extract day, month and year from datetime
        if day is None:
            visit_df['day']   = 1
        else:
            visit_df['day']   = day

        visit_df['month'] = month 
        visit_df['year']  = year 

        event_df['day']   = event_df['server_time'].dt.day
        event_df['month'] = event_df['server_time'].dt.month
        event_df['year']  = event_df['server_time'].dt.year
        
        # write to s3
        if not dry_run:

            partition_cols = ['idsite', 'year', 'month']

            if day is not None:
                partition_cols.append('day')

            res_visit = wr.s3.to_parquet(
                    df=visit_df,
                    path='s3://' + s3_visits_bucket,
                    dataset=True,
                    partition_cols=partition_cols)
            
            res_event = wr.s3.to_parquet(
                    df=event_df,
                    path='s3://' + s3_events_bucket,
                    dataset=True,
                    partition_cols=partition_cols)
                    

        else:
            s3logger.loginfo("Dry run, not writing to s3")
                       
     
    s3logger.loginfo("Ending procesing on datetime : %s site: %s year: %s month: %s day: %s" % ( datetime.datetime.now(), site, year, month, day))


def parse_args():

    parser = argparse.ArgumentParser(description="Usage Statistics Matomo mysql to S3 persistence", usage="python3 matomo2s3.py -s <site> -y <year> --from_month <month> --to_month <month> --from_day <day> --to_day <day>")

    parser.add_argument("--loglevel", 
                        help="Log level", 
                        default=logging.INFO)

    parser.add_argument("-c",
                        "--config_file_path",
                        default='config.ini',
                        help="config file",
                        required=False)

    parser.add_argument("-s",
                        "--site",
                        default='all',
                        help="site id",
                        required=True)

    parser.add_argument("-y",
                        "--year",
                        default=datetime.datetime.now().year,
                        type=int,
                        help="year",
                        required=True)
    
    parser.add_argument("-m",
                        "--month",
                        default=datetime.datetime.now().month,
                        type=int,
                        help="month",
                        required=True)

    parser.add_argument("-d",
                        "--day",
                        default=None,
                        type=int,
                        help="day",
                        required=False)
    
    parser.add_argument("-t",
                        "--type", 
                        default='R', 
                        type=str, 
                        help="(R|L|N)", 
                        required=False)

   
    parser.add_argument("-v", "--verbose", default=False, type=bool, help="verbose mode")


    parser.add_argument("--dry_run", default=False, type=bool, required=False, help="dont write to s3")

    args = parser.parse_args()

    return args 

  

if __name__ == "__main__":

    args = vars(parse_args())
    print("Arguments: ", args )     

    # set log level
    s3logger.set_log_level(args.get('loglevel'))
    
    if (args.get('verbose')):
        s3logger.set_log_level(logging.INFO)
        s3logger.set_local_mode(True)

    # call main function
    main(args)
