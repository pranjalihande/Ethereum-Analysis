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
    
    def good_line_for_transaction(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[7])
            float(fields[11])
            return True
        except:
            return False
        
    def good_line_for_scams(line):
        try:
            fields = line.split(',')
            if len(fields)!=4:
                return False
            return True
        except:
            return False

    lines_scams = spark.sparkContext.textFile("s3a://" + s3_bucket + "/scams.csv")
    clean_lines_scams=lines_scams.filter(good_line_for_scams)
    #(Address, (Category, Status, ID))
    scam_data = clean_lines_scams.map(lambda a: (a.split(',')[0], (a.split(',')[2], a.split(',')[3], a.split(',')[1]))) 

    lines_transaction = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines_transaction=lines_transaction.filter(good_line_for_transaction)
    # (To Address, (Value, Block_Timestamp) )       
    transaction_addr_val_data = clean_lines_transaction.map(lambda t: (t.split(',')[6], (float(t.split(',')[7]), time.strftime("%Y-%m", time.gmtime(float(t.split(',')[11]))))))
                                      
    joined_results = transaction_addr_val_data.join(scam_data)
    # [('0x01f7583ce239ad19682ccbadb21d69a03cf7be333f4be9c02233874e3f79bf9d', ((5000000000000000.0, '2015-08'), ('Phishing', 'Offline', '130')))   ('0x78e67bdd9e0bf6ce3cc6025e2ed9ee6d55828baae8d0c78dde0c1dd9fed0e33d', ((2e+19, '2015-08'), ('Phishing', 'Active', '1200')))]
                                      
                                      
    # The most lucrative form of scam:
    #(cat, value)
    cat_val = joined_results.map(lambda a: (a[1][1][0], a[1][0][0]))
    most_lucrative_scam_form = cat_val.reduceByKey(lambda a,b: a+b).sortBy(lambda a: -a[1])
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/most_lucrative_scam_form.txt')
    my_result_object.put(Body=json.dumps(most_lucrative_scam_form.collect()))
    
    # Category of scams changed through time: ((cat,time), value)
    get_cat = joined_results.map(lambda s: ((s[1][1][0], s[1][0][1]), s[1][0][0])).reduceByKey(operator.add)
    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/get_cat.txt')
    my_result_object.put(Body=json.dumps(get_cat.collect()))
    
    # Scams going offline/inactive? ((cat,status,Time), value)
    get_status = joined_results.map(lambda s: ((s[1][1][0], s[1][1][1], s[1][0][1]), s[1][0][0])).reduceByKey(operator.add)
    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/get_status_with_category_and_time.txt')
    my_result_object.put(Body=json.dumps(get_status.collect()))
    # ((cat,status), value)
    get_status_for_category = joined_results.map(lambda s: ((s[1][1][0], s[1][1][1]), s[1][0][0])).reduceByKey(operator.add)
    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/get_status_for_category.txt')
    my_result_object.put(Body=json.dumps(get_status_for_category.collect()))
    
    # The id of the most lucrative scam: (ID,value)
    most_lucarative_id = joined_results.map(lambda a: (a[1][1][2], a[1][0][0]))
    most_lucarative_id_val = most_lucarative_id.reduceByKey(operator.add).sortBy(lambda a: -a[1]).take(1)
    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/most_lucarative_id_val.txt')
    my_result_object.put(Body=json.dumps(most_lucarative_id_val))
    
    # The ether received has changed over time: (Time, Value)
    ether_recived = joined_results.map(lambda a: (a[1][0][1], a[1][0][0])).reduceByKey(operator.add).sortByKey(ascending=True)
    my_result_object = my_bucket_resource.Object(s3_bucket,'Test'  + '/ether_recived_over_time.txt')
    my_result_object.put(Body=json.dumps(ether_recived.collect()))

    spark.stop()
