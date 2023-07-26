import os
import time
import operator
import boto3
import json
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

#### Part A: Task1

    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=19:
                return False
            int(fields[16])
            int(fields[17])
            return True
        except:
            return False
    
    lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    clean_lines=lines.filter(good_line)
    
    #(Timestamp, Transaction count)
    time_transaction_data = clean_lines.map(lambda l: (l.split(',')[16], l.split(',')[17]))
    result = time_transaction_data.map(lambda t: (time.strftime("%Y-%m", time.gmtime(int(t[0]))), int(t[1])))
    final_result = result.reduceByKey(operator.add)
    transactions_per_month = final_result.sortByKey()

    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/final_result.txt')
    my_result_object.put(Body=json.dumps(transactions_per_month.collect()))
    
    
#### Part A: Task2 
    
    def good_line_price(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[7])
            float(fields[11])
            return True
        except:
            return False

    lines_transaction = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")

    clean_lines_transaction = lines_transaction.filter(good_line_price)
    #(Time, Value)
    time_value = clean_lines_transaction.map(lambda l: (float(l.split(',')[11]), float(l.split(',')[7])))
    final_time_value = time_value.map(lambda t: (time.strftime("%Y-%m", time.gmtime(float(t[0]))), (float(t[1]), 1)))
    result_price = final_time_value.reduceByKey(lambda p,c: (p[0] + c[0], p[1] + c[1])).map(lambda v: (v[0], (v[1][0] / v[1][1])))
    final = result_price.sortByKey(ascending=True)

    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/final_avg_result.txt')
    my_result_object.put(Body=json.dumps(final.collect()))
    
    spark.stop()