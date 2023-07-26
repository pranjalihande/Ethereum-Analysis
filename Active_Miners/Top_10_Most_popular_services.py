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
    
    def good_line_for_contracts(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
                return False
            return True
        except:
            return False
        
    def good_line_for_transaction(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[7])
            return True
        except:
            return False

    # (Address, 1)
    lines_contract = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    clean_lines_contract=lines_contract.filter(good_line_for_contracts)
    contract_addr_data = clean_lines_contract.map(lambda a: (a.split(',')[0], 1))

    # (TO Address, Value)
    lines_transaction = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines_transaction=lines_transaction.filter(good_line_for_transaction)
    transaction_addr_val_data = clean_lines_transaction.map(lambda t: (t.split(',')[6], float(t.split(',')[7])))
    
    results = transaction_addr_val_data.join(contract_addr_data)
    
    address_val_pair = results.reduceByKey(lambda x, y: (float(x[0]) + float(y[0]), x[1]+y[1]))
    top_address=address_val_pair.map(lambda a: (a[0], a[1][0]))
    # top 10 addresses with values
    top10_address=top_address.takeOrdered(10, key=lambda x: -x[1])

    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/part_B_top_10.txt')
    my_result_object.put(Body=json.dumps(top10_address))
    
    spark.stop()
