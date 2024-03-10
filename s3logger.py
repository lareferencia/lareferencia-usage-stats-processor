from cmath import log
import io
import logging
import boto3  
import datetime

class S3Logger:
    def __init__(self, appname, loglevel=logging.INFO, bucket=None, local_mode = False):
        self.bucket = bucket
        self.s3 = boto3.client('s3')
        self.log_stringio = io.StringIO()
        self.logger = logging.getLogger()
        self.logger.addHandler(logging.StreamHandler(self.log_stringio))
        self.logger.setLevel(loglevel) 
        self.appname = appname
        self.local_mode = local_mode

        logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)


    def set_bucket(self, bucket):
        self.bucket = bucket  

    def set_log_level(self, loglevel):
        self.logger.setLevel(loglevel)

    def set_local_mode(self, local_mode):
        self.local_mode = local_mode

    def loginfo(self, message):
        if self.local_mode:
            print(message)
                 
        self.logger.info(message)

    def logwarning(self, message):

        if self.local_mode:
            print(message)
    
        self.logger.warning(message)

    def logerror(self, message):
        if self.local_mode:
            print(message)
        
        self.logger.error(message)

    def logdebug(self, message):
        if self.local_mode:
            print(message)
        
        self.logger.debug(message)

    def write(self,name):
        
        if ( self.bucket is None ):
            self.logerror("No bucket specified")
            return

        now = datetime.datetime.now()
        year, month, day = now.year, now.month, now.day
        time = now.strftime("%H_%M_%S") 

        key = self.appname + "/" + str(year) + "/" + str(month) + "/" + str(day) + "/" + name + "_" + time + ".log"

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=self.log_stringio.getvalue())
        
        self.log_stringio.truncate(0)

    def close(self):
        self.logger.removeHandler(self.log_stringio)
        self.log_stringio.close()   