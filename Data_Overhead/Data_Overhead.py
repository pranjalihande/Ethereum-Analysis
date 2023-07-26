import sys, string
import os
import socket
import time
import operator
import boto3
import json
import ast
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("ethereum")\
        .getOrCreate()
        
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
 
        
    def get_size(column_data):
        if len(column_data) > 0:
            size = (len(column_data)-2) * 4
            return size
        else:
            return 0
        
    def good_line_for_blocks(line):
        try:
            fields = line.split(',')
            if len(fields)!=19:
                return False
            return True
        except:
            return False

    lines_contract = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    clean_lines_contract=lines_contract.filter(good_line_for_blocks)
    
    # (logs_bloom, sha3_uncles, transactions_root, state_root, and receipts_root),
    contract_addr_data = clean_lines_contract.map(lambda a: ("Key", (get_size(a.split(',')[5]), get_size(a.split(',')[4]), get_size(a.split(',')[6]), get_size(a.split(',')[7]), get_size(a.split(',')[8]))))
              
    result = contract_addr_data.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3], x[4] + y[4]))    
    final_result = result.map(lambda x: (x[0], (x[1][0] + x[1][1] + x[1][2] + x[1][3] + x[1][4])))

    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/saved_space.txt')
    my_result_object.put(Body=json.dumps(final_result.collect()))

    
    spark.stop()
