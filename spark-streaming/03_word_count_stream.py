from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("stream-word-count").getOrCreate()

lines_df = spark.readStream.format("socket").option("host", "localhost").option("port", "9999").load()

# explode : 배열로 된 데이터를 풀어줍니다. flat과 같습니다.
words_df = lines_df.select(expr("explode(split(value, ' ')) as word"))
counts_df = words_df.groupBy("word").count()

word_count_query = counts_df.writeStream.format("console")\
                            .outputMode("complete")\
                            .option("checkpointLocation", ".checkpoint")\
                            .start()
word_count_query.awaitTermination()