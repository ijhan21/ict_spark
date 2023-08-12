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
    
    # inner, left, right, full outer join(streaming에서는 사용이 불가능)
    authors_books_df = authors.join(
        books,
        authors["book_id"] == books["id"],
        "inner"
    )

    authors_books_df.show()

    # load 까지 하면 value컬럼의 데이터 생성됨
    def stream_join_with_static():
        streamed_books = spark.readStream\
                                .format("socket")\
                                .option("host", "localhost")\
                                .option("port", 12345)\
                                .load()\
                                .select(F.from_json(F.col("value"), books.schema).alias("book"))\
                                .selectExpr("book.id as id", "book.name as name", "book.year as year")
        # 이전에 만들어 놓은 books의 schema를 재활용 하기로 한다
        
        # 왼쪽이 static, 오른쪽이 stream -> left, full outer join은 불가능
        # authors_books_df = authors.join(
        #     streamed_books,
        #     authors["book_id"] == streamed_books["id"],
        #     # "inner"
        #     # "left" # 안됨
        #     "right"
        # )
        # authors_books_df.writeStream.format("console").outputMode("append").start().awaitTermination()
        # 왼쪽이 stream, 오른쪽이 static -> right, full outer join은 불가능
        authors_books_df = streamed_books.join(
            authors,
            authors["book_id"] == streamed_books["id"],
            # "inner"
            # "left" # 안됨
            "left"
        )
        authors_books_df.writeStream.format("console").outputMode("append").start().awaitTermination()
    # stream_join_with_static()

    # 양쪽이 다 stream인 경우
    def stream_join_with_stream():
        streamed_books = spark.readStream\
                                .format("socket")\
                                .option("host", "localhost")\
                                .option("port", 9999)\
                                .load()\
                                .select(F.from_json(F.col("value"), books.schema).alias("book"))\
                                .selectExpr("book.id as id", "book.name as name", "book.year as year")
        streamed_authors = spark.readStream\
                                .format("socket")\
                                .option("host", "localhost")\
                                .option("port", 9998)\
                                .load()\
                                .select(F.from_json(F.col("value"), authors.schema).alias("author"))\
                                .selectExpr("author.id as id", "author.name as name", "author.book_id as book_id")
        
        authors_books_df = streamed_authors.join(
            streamed_books,
            streamed_authors["book_id"]==streamed_books["id"],
            "inner"
        )
        authors_books_df.writeStream.format("console").outputMode("append").start().awaitTermination()
    stream_join_with_stream()