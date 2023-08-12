from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Streaming DataFrame Join")\
        .master("local[2]")\
        .getOrCreate()
    
    # 1. 기본 JOIN
    authors = spark.read\
        .option("interSchema", True)\
        .json("/home/ubuntu/working/spark-examples/spark-streaming/data/authors.json")
    
    books = spark.read\
        .option("interSchema", True)\
        .json("/home/ubuntu/working/spark-examples/spark-streaming/data/books.json")
    