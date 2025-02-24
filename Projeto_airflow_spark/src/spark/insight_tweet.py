from pyspark.sql import  SparkSession
from pyspark.sql import functions as f
from os.path import join
import argparse 

def get_tweets_conversas(df_tweet):
    tweet_conversas = df_tweet.alias("tweet")\
                    .groupBy(f.to_date("created_at").alias('create_date'))\
                    .agg(
                        f.count_distinct("author_id").alias("n_twwits"),
                        f.sum("like_count").alias("n_like"),
                        f.sum("quote_count").alias("n_quote"),
                        f.sum("reply_count").alias("n_reply"),
                        f.sum("retweet_count").alias("n_retweet")
                    ).withColumn("weekday",f.date_format("create_date","E"))
    return tweet_conversas
    
def export_json(df,dest):
    df.coalesce(1).write.mode("overwrite").json(dest)    


def twitter_insight(spark, src, dest, process_date):
    

    df_tweet = spark.read.json(join(src,"tweet"))

    tweet_conversas=get_tweets_conversas(df_tweet)

    export_json(tweet_conversas,join(dest,f"process_date={process_date}"))

    
    
if __name__=="__main__":  

    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"        
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest",required=True)
    parser.add_argument("--process_date",required=True)

    args = parser.parse_args()

    spark = SparkSession\
    .builder\
    .appName("twitter_trasformation_insigth")\
    .getOrCreate()   
    
    twitter_insight(spark,args.src,args.dest,args.process_date)
    
    