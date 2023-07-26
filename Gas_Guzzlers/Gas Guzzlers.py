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

    def good_line_price(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[9])
            float(fields[11])
            float(fields[8])
            float(fields[7])
            return True
        except:
            return False
        
    def good_line_for_contracts(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
                return False
            return True
        except:
            return False
        
    lines_contract = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    clean_lines_contract=lines_contract.filter(good_line_for_contracts)
    contract_addr_data = clean_lines_contract.map(lambda a: (a.split(',')[0], 1))
    
    lines_transaction = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines_transaction = lines_transaction.filter(good_line_price)
    transaction_data = clean_lines_transaction.map(lambda l: (l.split(',')[6], (float(l.split(',')[9]), float(l.split(',')[8]), time.strftime("%Y-%m", time.gmtime(float(l.split(',')[11]))), float(l.split(',')[7]))))
    
    # (address, gas_price, gas, time, value)
    joined_results = transaction_data.join(contract_addr_data)
    # join addr result: [('0x01f7583ce239ad19682ccbadb21d69a03cf7be333f4be9c02233874e3f79bf9d', ((500000000000.0, 21000.0, '2015-08'), 1)),      ('0x78e67bdd9e0bf6ce3cc6025e2ed9ee6d55828baae8d0c78dde0c1dd9fed0e33d', ((500000000000.0, 21000.0, '2015-08'), 1))]

    #(Time, gas price)
    average_gas = joined_results.map(lambda x: (x[1][0][2], (x[1][0][0], 1)))
    average_gas_used_per_month = average_gas.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).map(lambda x:(x[0], x[1][0]/x[1][1]))
    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/average_gas_used_per_month.txt')
    my_result_object.put(Body=json.dumps(average_gas_used_per_month.collect()))
    
     #(Time, Gas)
    time_with_gas_used = joined_results.map(lambda x: (x[1][0][2], x[1][0][1])).reduceByKey(operator.add)
    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/time_with_gas_used.txt')
    my_result_object.put(Body=json.dumps(time_with_gas_used.collect()))
    
    #(Time, value)
    time_with_values_used = joined_results.map(lambda x: (x[1][0][2], x[1][0][3])).reduceByKey(operator.add).sortBy(lambda a: -a[1])
    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/time_with_values_used.txt')
    my_result_object.put(Body=json.dumps(time_with_values_used.collect()))
    
    #(Gas, overall avg)
    average_gas = joined_results.map(lambda x: ('key', (x[1][0][1], 1)))
    total_average_gas_used = average_gas.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).map(lambda x:(x[0], x[1][0]/x[1][1]))
    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/total_average_gas_used.txt')
    my_result_object.put(Body=json.dumps(total_average_gas_used.collect()))
    
    # (address, (value, gas, 1))
    most_popular_contract_with_gas = joined_results.map(lambda l: (l[0],(l[1][0][3],l[1][0][1],1)))
    most_popular_contract_with_gas_data = most_popular_contract_with_gas.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
    # (address, (value, gas/count)), sorted with value
    average_gas_with_popular_contracts = most_popular_contract_with_gas_data.map(lambda x: (x[0],(x[1][0], x[1][1] / x[1][2]))).sortBy(lambda a: -a[1][0])
    # Top 10 contracts with highest value first
    top10_address_avg_gas_value = average_gas_with_popular_contracts.takeOrdered(10, key = lambda x: -x[1][0])
    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/top10address_avg_gas_value.txt')
    my_result_object.put(Body=json.dumps(top10_address_avg_gas_value))
    
    spark.stop()