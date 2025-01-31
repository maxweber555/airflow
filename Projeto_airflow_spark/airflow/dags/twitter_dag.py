
import sys
sys.path.append("/home/maxweber/Notbook/airflow")
from airflow.models import DAG
from os.path import join
from operators.twitter_operator import TwitterOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pathlib import Path


with DAG(dag_id = "TwitterDag",
         start_date=days_ago(6),
         schedule_interval = "@daily"
) as dag:
        
        BASE_FOLDER = join(str(Path("~/Notbook").expanduser()),
                           "datalake/{stage}/twitter_datascience/{partition}",
        )

        PARTITION_FOLDER_EXTRACT = "extract_date={{data_interval_start.strftime('%Y-%m-%d') }}"

        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
        query = "datascience"
        twitter_operator = TwitterOperator(file_path = join(BASE_FOLDER.format(stage="Bronze",partition=PARTITION_FOLDER_EXTRACT ),
                                              "datescience_{{ ds_nodash }}.json"),
                                              query=query,
                                              start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                                              end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                                              task_id = "twitter_datascience")
        
        twitter_transform = SparkSubmitOperator(
                task_id = "twitter_transform_datascience",
                application="/home/maxweber/Notbook/src/spark/trasformation.py",
                name="twwitter_trasformation",
                application_args=["--src",BASE_FOLDER.format(stage="Bronze",partition=PARTITION_FOLDER_EXTRACT ),
                                   "--dest",BASE_FOLDER.format(stage="Silver",partition="" ),
                                   "--process_date","{{ ds }}"])

        twitter_insight = SparkSubmitOperator(
                task_id = "insight_tweet",
                application="/home/maxweber/Notbook/src/spark/insight_tweet.py",
                name="insight_tweet",
                application_args=["--src",BASE_FOLDER.format(stage="Silver",partition="" ),
                                   "--dest",BASE_FOLDER.format(stage="Gold",partition="" ),
                                   "--process_date","{{ ds }}"])

twitter_operator >> twitter_transform >> twitter_insight    