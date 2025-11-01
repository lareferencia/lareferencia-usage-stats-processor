import logging
import os
import sys
import argparse
from calendar import monthrange
from unicodedata import name
import requests
import json
import gc
import psutil
import tracemalloc

import pymysql
import awswrangler as wr
import pandas as pd

import datetime

from config import read_ini

import xxhash
import atexit
from s3logger import S3Logger 

# logger for s3
s3logger = S3Logger('matomo2parquet');

def log_memory_usage(stage, debug_mode=False):
    """
    Log memory usage at different stages of processing
    """
    if debug_mode:
        # Get memory info
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        
        # Convert bytes to MB
        rss_mb = memory_info.rss / 1024 / 1024
        vms_mb = memory_info.vms / 1024 / 1024
        
        # Get system memory info
        system_memory = psutil.virtual_memory()
        available_mb = system_memory.available / 1024 / 1024
        
        memory_msg = f"[MEMORY {stage}] RSS: {rss_mb:.2f}MB, VMS: {vms_mb:.2f}MB, Percent: {memory_percent:.2f}%, Available: {available_mb:.2f}MB"
        
        s3logger.loginfo(memory_msg)
        
        if debug_mode:
            print(memory_msg)
        
        # Force garbage collection if memory usage is high
        if memory_percent > 70:
            s3logger.logwarning(f"High memory usage detected ({memory_percent:.2f}%), forcing garbage collection")
            collected = gc.collect()
            s3logger.loginfo(f"Garbage collector freed {collected} objects")
            
        # Emergency mode if memory usage is extremely high
        if memory_percent > 85:
            s3logger.logerror(f"CRITICAL: Memory usage extremely high ({memory_percent:.2f}%)")
            s3logger.logerror("Consider using smaller chunk_size or processing data in smaller date ranges")
            
        return rss_mb, memory_percent
    return None, None

def optimize_dataframe_memory(df, debug_mode=False):
    """
    Optimize DataFrame memory usage by downcasting numeric types
    """
    if df.empty:
        return df
        
    if debug_mode:
        original_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
        s3logger.loginfo(f"Original DataFrame memory: {original_memory:.2f}MB")
    
    # Downcast integer columns
    for col in df.select_dtypes(include=['int']).columns:
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    # Downcast float columns
    for col in df.select_dtypes(include=['float']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    # Convert object columns to category if they have few unique values
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].nunique() / len(df) < 0.5:  # Less than 50% unique values
            df[col] = df[col].astype('category')
    
    if debug_mode:
        optimized_memory = df.memory_usage(deep=True).sum() / 1024 / 1024
        saved_memory = original_memory - optimized_memory
        s3logger.loginfo(f"Optimized DataFrame memory: {optimized_memory:.2f}MB (saved: {saved_memory:.2f}MB)")
    
    return df

def get_dataframe_memory_usage(df, name):
    """
    Get detailed memory usage of a DataFrame
    """
    memory_usage = df.memory_usage(deep=True).sum()
    memory_mb = memory_usage / 1024 / 1024
    rows, cols = df.shape
    
    msg = f"[DF MEMORY] {name}: {memory_mb:.2f}MB, Shape: ({rows}, {cols})"
    s3logger.loginfo(msg)
    print(msg)
    
    return memory_mb

def read_sql_in_chunks(query, connection, chunk_size=10000, debug_mode=False):
    """
    Read SQL query in chunks to reduce memory usage
    """
    if debug_mode:
        s3logger.loginfo(f"Reading SQL in chunks of {chunk_size} rows")
    
    try:
        # Try to read with chunksize parameter
        chunk_iter = pd.read_sql(query, connection, chunksize=chunk_size)
        chunks = []
        total_rows = 0
        
        for i, chunk in enumerate(chunk_iter):
            if debug_mode and i % 10 == 0:  # Log every 10 chunks
                log_memory_usage(f"PROCESSING_CHUNK_{i}", debug_mode)
            
            chunks.append(chunk)
            total_rows += len(chunk)
            
            # If we accumulate too many chunks, combine them and clear memory
            if len(chunks) >= 50:  # Combine every 50 chunks
                if debug_mode:
                    s3logger.loginfo(f"Combining {len(chunks)} chunks...")
                combined_chunk = pd.concat(chunks, ignore_index=True)
                chunks = [combined_chunk]
                gc.collect()
        
        if debug_mode:
            s3logger.loginfo(f"Total rows read: {total_rows}")
        
        if chunks:
            result = pd.concat(chunks, ignore_index=True)
            del chunks
            gc.collect()
            return result
        else:
            return pd.DataFrame()
            
    except Exception as e:
        if debug_mode:
            s3logger.logwarning(f"Chunked reading failed: {e}, falling back to regular read")
        # Fallback to regular read
        return wr.mysql.read_sql_query(sql=query, con=connection)

def main(args_dict):

    config_file_path = args_dict.get('config_file_path', None)

    site = args_dict.get('site', None)
    year = args_dict.get('year', None) 
    month = args_dict.get('month', None)
    day = args_dict.get('day', None)

    dry_run = args_dict.get('dry_run', False)
    debug_mode = args_dict.get('debug', False)
    
    # Start memory tracking if debug mode is enabled
    if debug_mode:
        tracemalloc.start()
        s3logger.loginfo("DEBUG MODE: Memory tracking enabled")
        
    log_memory_usage("STARTUP", debug_mode)

    try: 
        # read config file
        config = read_ini(config_file_path);
        
        log_memory_usage("CONFIG_LOADED", debug_mode)

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
        log_memory_usage("BEFORE_DB_CONNECTION", debug_mode)
        conn = pymysql.connect(host=db_host, user=db_username, passwd=db_passwd, db=db_database, connect_timeout=5)
        log_memory_usage("AFTER_DB_CONNECTION", debug_mode)
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
    if debug_mode:
        s3logger.loginfo(f"Executing visit query: {visit_query}")
    
    log_memory_usage("BEFORE_VISIT_QUERY", debug_mode)
    
    # Use chunked reading for large datasets
    chunk_size = args_dict.get('chunk_size', 10000)
    if debug_mode:
        visit_df = read_sql_in_chunks(visit_query, conn, chunk_size, debug_mode)
    else:
        visit_df = wr.mysql.read_sql_query(sql=visit_query,con=conn)
        
    log_memory_usage("AFTER_VISIT_QUERY", debug_mode)
    
    if not visit_df.empty and debug_mode:
        get_dataframe_memory_usage(visit_df, "visit_df")
        # Optimize memory usage
        visit_df = optimize_dataframe_memory(visit_df, debug_mode)
    
    if debug_mode:
        s3logger.loginfo(f"Executing event query: {event_query}")
    
    log_memory_usage("BEFORE_EVENT_QUERY", debug_mode)
    
    # Use chunked reading for large datasets
    if debug_mode:
        event_df = read_sql_in_chunks(event_query, conn, chunk_size, debug_mode)
    else:
        event_df = wr.mysql.read_sql_query(sql=event_query,con=conn)
        
    log_memory_usage("AFTER_EVENT_QUERY", debug_mode)
    
    if not event_df.empty and debug_mode:
        get_dataframe_memory_usage(event_df, "event_df")
        # Optimize memory usage
        event_df = optimize_dataframe_memory(event_df, debug_mode)

    if visit_df.empty:
        s3logger.logwarning("No data for site: %s year: %s month: %s day: %s" % (site, year, month, day))
    else:
        log_memory_usage("BEFORE_DATA_PROCESSING", debug_mode)
        
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
        
        log_memory_usage("AFTER_DATA_PROCESSING", debug_mode)
        
        if debug_mode:
            get_dataframe_memory_usage(visit_df, "visit_df_processed")
            get_dataframe_memory_usage(event_df, "event_df_processed")
        
        # write to s3
        if not dry_run:

            partition_cols = ['idsite', 'year', 'month']

            if day is not None:
                partition_cols.append('day')

            log_memory_usage("BEFORE_S3_WRITE", debug_mode)
            
            s3logger.loginfo("Writing visit data to S3...")
            res_visit = wr.s3.to_parquet(
                    df=visit_df,
                    path='s3://' + s3_visits_bucket,
                    dataset=True,
                    partition_cols=partition_cols)
            
            log_memory_usage("AFTER_VISIT_S3_WRITE", debug_mode)
            
            # Clear visit_df from memory to free space
            if debug_mode:
                s3logger.loginfo("Clearing visit_df from memory...")
            del visit_df
            gc.collect()
            
            log_memory_usage("AFTER_VISIT_DF_CLEANUP", debug_mode)
            
            s3logger.loginfo("Writing event data to S3...")
            res_event = wr.s3.to_parquet(
                    df=event_df,
                    path='s3://' + s3_events_bucket,
                    dataset=True,
                    partition_cols=partition_cols)
            
            log_memory_usage("AFTER_EVENT_S3_WRITE", debug_mode)
            
            # Clear event_df from memory to free space
            if debug_mode:
                s3logger.loginfo("Clearing event_df from memory...")
            del event_df
            gc.collect()
            
            log_memory_usage("AFTER_EVENT_DF_CLEANUP", debug_mode)

        else:
            s3logger.loginfo("Dry run, not writing to s3")
                       
    log_memory_usage("BEFORE_CLEANUP", debug_mode)
    
    # Close database connection
    if 'conn' in locals():
        conn.close()
        
    # Final cleanup
    gc.collect()
    
    log_memory_usage("AFTER_FINAL_CLEANUP", debug_mode)
    
    # Show memory usage summary if debug mode is enabled
    if debug_mode:
        if tracemalloc.is_tracing():
            current, peak = tracemalloc.get_traced_memory()
            s3logger.loginfo(f"MEMORY SUMMARY - Current: {current / 1024 / 1024:.2f}MB, Peak: {peak / 1024 / 1024:.2f}MB")
            tracemalloc.stop()
                       
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

    parser.add_argument("--debug", default=False, action='store_true', help="enable debug mode with detailed memory monitoring")

    parser.add_argument("--chunk_size", default=10000, type=int, help="chunk size for reading large datasets (default: 10000)")

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
