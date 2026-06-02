# spark_config.py — partagé par toute l'équipe
from pyspark.sql import SparkSession
import os

def get_spark_session(app_name="esg-pipeline", executor_memory="4g"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.hadoop.fs.s3a.endpoint",          "https://minio.lab.sspcloud.fr")
        .config("spark.hadoop.fs.s3a.access.key",        os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key",        os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.sql.parquet.filterPushdown",      "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
