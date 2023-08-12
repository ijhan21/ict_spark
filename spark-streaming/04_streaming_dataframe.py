from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Streaming DataFrame")\
        .master("local[2]")\
        .getOrCreate()

    # Schema 정의
    # StructType은 전체 구조
    # StructField 는 컬럼
    schema = StructType([
        StructField("ip", StringType(), False), # 이름, 타임, null 허용
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False),
    ])

    def hello_streaming():
        df = spark.readStream.format("socket")\
        .option("host", "localhost")\
        .option("port",12345)\
        .load()

        df.writeStream.format("console").outputMode("append")\
            .trigger(processingTime = "2 seconds")\
            .start().awaitTermination()
    # hello_streaming()

    def read_from_socket():
        line = spark.readStream.format("socket")\
        .option("host", "localhost")\
        .option("port",12345)\
        .load()
        cols = ["ip", "timestamp", "method", "endpoint", "status_code", "latency"]
        df = line.withColumn("ip", F.split(line['value'],",").getItem(0))\
                .withColumn("timestamp", F.split(line['value'],",").getItem(1))\
                .withColumn("method", F.split(line['value'],",").getItem(2))\
                .withColumn("endpoint", F.split(line['value'],",").getItem(3))\
                .withColumn("status_code", F.split(line['value'],",").getItem(4))\
                .withColumn("latency", F.split(line['value'],",").getItem(5))\
                .select(cols)
        
        # filter : status_code가 400이고, endpoint가 /users인 row만 필터링
        # df.filter((df.status_code == "400") & (df.endpoint == "/users"))
        df = df.filter((F.col('status_code') == "400") & (F.col('endpoint') == "/events"))

        # group by : method, endpoint 별 latency의 최댓값, 최솟값, 평균값
        # 주의 할 점
        # append mode에서는 watermark를 지정하지 않으면 DataFrame의 Aggregation을 지원하지 않는다.
        # 왜? 집계는 과거와 현재 데이터를 이용해서 처리하는 과정이기 때문에..
        # 하지만 append는 새롭게 추가되는 데이터에 대한 처리를 의미.
        # 만약 집계와 append를 허용한다면 spark는 미래의 무한 스트림의 전체 상태를 알고 있어야 한다는 말이 된다.
        df = df.groupby(["method","endpoint"])\
                .agg(
                    F.max(F.col("latency")).alias("max_latency"),
                    F.min(F.col("latency")).alias("max_latency"),
                    F.mean(F.col("latency")).alias("max_latency"),
                )

        # df.writeStream.format("console").outputMode("append")\
        df.writeStream.format("console").outputMode("complete")\
            .start().awaitTermination()
    # read_from_socket()

    # read_from_socket()
    def read_from_files():
        # filepath : directory만 가능
        logs_df = spark.readStream.format("csv")\
            .option("header", "false")\
            .schema(schema)\
            .load("data/logs")
        logs_df.writeStream.format("console")\
        .outputMode("append")\
        .start().awaitTermination()

    read_from_files()