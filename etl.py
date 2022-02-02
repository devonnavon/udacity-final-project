
from datetime import datetime
import os
import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_timestamp, col
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek


def create_spark_session():
    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
    
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()
    return spark

def process_data(spark, input_data, output_data):

    meta_path = os.path.join(input_data, 'coin_meta_data.csv')
    prices_path = os.path.join(input_data, 'coin_price_data/') 
    gtrends_path = os.path.join(input_data, 'google_trends_data/')

    #process coins meta data
    pd_raw_meta_df = pd.read_csv(meta_path) #pandas deals with intrafield commas better that spark
    raw_meta_df = spark.createDataFrame(pd_raw_meta_df.where(pd.notnull(pd_raw_meta_df), None)) #relpalce nan w/ null so spark can read it in
    coins_table = raw_meta_df.selectExpr(['id as coin_id', 'symbol as ticker', 'name', 'description', 'twitter_screen_name as twitter_account', 'subreddit_url', 'github_url'])

    coins_table_out_path = os.path.join(output_data, 'coins')
    coins_table.write.mode('overwrite').parquet(coins_table_out_path)
    print(f'coins table saved to {coins_table_out_path}')


    # process coin metrics data
    raw_prices_df = spark.read.csv(prices_path, header=True)
    coin_metrics_table = raw_prices_df.withColumn('currency', lit('usd'))\
        .withColumn('recorded_at',to_timestamp('date'))\
        .withColumn('price', raw_prices_df['price_usd'].cast(DoubleType()))\
        .withColumn('market_cap', raw_prices_df['mcap_usd'].cast(DoubleType()))\
        .withColumn('volume', raw_prices_df['volume_usd'].cast(DoubleType()))\
        .select('coin_id', 'recorded_at', 'currency', 'price', 'market_cap', 'volume').drop_duplicates()

    check_coin_metrics_quality(coin_metrics_table) # run quality check
    
    #save to outfolder
    coin_metrics_table_out_path = os.path.join(output_data, 'coin_metrics')
    coin_metrics_table.write.mode('overwrite').parquet(coin_metrics_table_out_path)
    print(f'coin metrics table saved to {coins_table_out_path}')

    #process trends data
    raw_gtrends_df = spark.read.csv(gtrends_path, header=True)
    google_trends_table = raw_gtrends_df.withColumn('recorded_at',to_timestamp('date'))\
        .withColumn('trend_value', raw_gtrends_df['keyword_interest'].cast(IntegerType()))\
        .select('coin_id', 'recorded_at', 'keyword', 'trend_value').drop_duplicates()
    
    check_google_trends_quality(google_trends_table) #run quality check

    google_trends_table_out_path = os.path.join(output_data, 'google_trends')
    google_trends_table.write.mode('overwrite').parquet(google_trends_table_out_path)
    print(f'google trends table saved to {google_trends_table_out_path}')

    #build time table
    time_table = coin_metrics_table.select('recorded_at').union(google_trends_table.select('recorded_at'))\
        .drop_duplicates()\
        .withColumn('hour', hour('recorded_at'))\
        .withColumn('day', dayofmonth('recorded_at'))\
        .withColumn('week', weekofyear('recorded_at'))\
        .withColumn('month', month('recorded_at'))\
        .withColumn('year', year('recorded_at'))\
        .withColumn('weekday', dayofweek('recorded_at'))

    time_table_table_out_path = os.path.join(output_data, 'time_table')
    time_table.write.mode('overwrite').parquet(time_table_table_out_path)
    print(f'time table saved to {time_table_table_out_path}')

def check_coin_metrics_quality(df):
    duplicate_count = df.groupBy('coin_id', 'recorded_at').count().where(col('count') > 1).count()
    if duplicate_count > 0:
        raise Exception('duplicates exist in coin metrics table')

    null_count = df.where(
        (col('coin_id').isNull()) |
        (col('recorded_at').isNull()) |
        (col('price').isNull()) |
        (col('market_cap').isNull()) |
        (col('volume').isNull())).count()

    if null_count > 0:
        raise Exception('nulls exist in coin metrics table')

def check_google_trends_quality(df):
    duplicate_count = df.groupBy('coin_id', 'recorded_at','keyword', 'trend_value').count().where(col('count') > 1).count()
    if duplicate_count > 0:
        raise Exception('duplicates exist in google trends table')

    null_count = df.where(
        (col('coin_id').isNull()) |
        (col('recorded_at').isNull()) |
        (col('keyword').isNull()) |
        (col('trend_value').isNull())).count()

    if null_count > 0:
        raise Exception('nulls exist in coin metrics table')

def main():
    spark = create_spark_session()
    
    #to run locally
    input_data = "input_data/"
    output_data = "output_data/"

    #run etl
    process_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()