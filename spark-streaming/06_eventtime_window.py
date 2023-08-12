from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import pyspark.sql.functions as F

if __name__ == "__main__":
    
    spark = SparkSession.builder.master("local[2]").appName("Event Time Windows").getOrCreate()
    
    domain_traffic_schema = StructType([
        StructField("id", StringType(), False),
        StructField("domain", StringType(), False),
        StructField("count", IntegerType(), False),
        StructField("time", TimestampType(), False),
    ])
    
    def read_traffics_from_socket():
        return spark.readStream\
            .format("socket")\
            .option("host", "localhost")\
            .option("port", 12345).load()\
            .select(F.from_json(F.col("value"),
                                domain_traffic_schema)\
                    .alias("traffic"))\
            .selectExpr("traffic.*")
    
    # 슬라이딩 윈도우 생성
    # duration, interval을 각각 다르게 설정
    def aggregate_traffic_counts_by_sliding_window():
        traffics_df = read_traffics_from_socket()

        # F.window : 윈도우를 생성할 컬럼.
        # 시간 개념을 사용하려면 반드시 윈도우가 필요하다!!
        #입력을 굳이 문자열로 넣는다
        # 윈도우는 순차적으로 만드는 것이 아니다. 이미 만들어져 있고 그 윈도우에 맞춰서 집계가 이루어진다.
        window_by_hours = traffics_df\
        .groupby(
            F.window(timeColumn=F.col("time"), windowDuration="2 hours", slideDuration="1 hour").alias("time"))\
                .agg(
                    F.sum("count").alias("total_count")
                )
       
        window_by_hours.writeStream.format("console").outputMode("complete").start().awaitTermination()
    # aggregate_traffic_counts_by_sliding_window()
    def aggregate_traffic_counts_by_sliding_window_start_end():
        traffics_df = read_traffics_from_socket()

        # F.window : 윈도우를 생성할 컬럼.
        # 시간 개념을 사용하려면 반드시 윈도우가 필요하다!!
        #입력을 굳이 문자열로 넣는다
        # 윈도우는 순차적으로 만드는 것이 아니다. 이미 만들어져 있고 그 윈도우에 맞춰서 집계가 이루어진다.
        window_by_hours = traffics_df\
        .groupby(
            F.window(timeColumn=F.col("time"), windowDuration="2 hours", slideDuration="1 hour").alias("time"))\
                .agg(
                    F.sum("count").alias("total_count")
                )\
                .select(
                    F.col("time").getField("start").alias("start"),
                    F.col("time").getField("end").alias("end"),
                    F.col("total_count")
                ).orderBy(F.col("start"))
       
        window_by_hours.writeStream.format("console").outputMode("complete").start().awaitTermination()
    aggregate_traffic_counts_by_sliding_window_start_end()