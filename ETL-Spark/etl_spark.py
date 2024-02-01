import requests
import xmltodict
from dotenv import load_dotenv
import os
import csv
from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import col,regexp_replace


load_dotenv()

URL=os.getenv('URL')


def extract_data():
    xml_data=requests.get(URL)
    data01=xmltodict.parse(xml_data.text)
    data=data01['rss']['channel']['item']
    return data

def load_into_csv(data):
    file_path='transformed_data.csv'
    field_name=['title' ,'author', 'publish_date','job_summary']
    with open(file_path, 'w' ,newline='', encoding='utf-8')as csv_file:
        csv_writer=csv.DictWriter(csv_file,fieldnames=field_name)
        csv_writer.writeheader()
        
        for d in data:
            csv_writer.writerow({
                'title':d.get('title',''),
                'author': d.get('itunes:author'),
                'publish_date' : d.get('pubDate',''),
                'job_summary':d.get('itunes:summary','')

                
            })
            
def transform_csv():
    spark=pyspark.sql.SparkSession.builder.getOrCreate()
    spark_df=spark.read.csv('transformed_data.csv',header=True)
    spark_df=spark_df.drop('author')
    df = spark_df.withColumn("publish_date_01", col("publish_date").substr(1, 16))
    df = df.withColumn("job_summary01", regexp_replace(col("job_summary"), "<[^>]*>", ""))
    df = df.withColumn("title_01",regexp_replace(
                       regexp_replace(
                           regexp_replace(col("title"), "<[^>]*>", ""),
                           '<div>.*?</div>', ''),
                        '<div class="p-rich_text_section">', ''
                           )
                       )
    df = df.drop("publish_date")
    df=  df.drop('job_summary')
    df=  df.drop('title_01')
    df = df.withColumnRenamed("publish_date_01", "publish_date")
    df = df.withColumnRenamed("job_summary01", "job_summary")
    df = df.filter((col("publish_date") != "") & (col("job_summary") != "") & (col("publish_date").isNotNull()) & (col("job_summary").isNotNull()))
    return df
    
def load(data):
    data.write.csv('cleaned_csv02.csv', header=True, mode="overwrite")


data=extract_data()
transformed_data=load_into_csv(data)
clean_data=transform_csv()         
load(clean_data)            
            
  