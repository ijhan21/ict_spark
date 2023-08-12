from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# RDD를 다루는거는 Context인가?
# SparkContext를 생성하는데 세션 사용하기 -> 최신 코드

if __name__ == "__main__":
    # SparkContext 생성 From SparkSession
    # 2개의 cpu를 쓰겠다 표현. spark 공식문서에서 최소 2개는 해야 된다고 나옴. [*]로 쓰면 모든 cpu활용한다는 뜻
    sc = SparkSession.builder \
        .master("local[2]").appName("DStream Example").getOrCreate().sparkContext
    
    # Streaming Context
    ssc = StreamingContext(sc, 5) # 5초마다 새로운 마이크로 배치를 실행

    def read_from_socket():
        socket_stram = ssc.socketTextStream("localhost", 12345) #들어오는 데이터를 모두 text로 처리하는 스트림

        # Transformation
        words_stream = socket_stram.flatMap(lambda line : line.split())

        # Action
        words_stream.pprint()

        # 스트림 대기
        ssc.start()
        ssc.awaitTermination() # 종료 할 때까지 대기
    
    def read_from_file():
        stocks_file_path = "file:///home/ubuntu/working/spark-examples/spark-streaming/data/stocks"
        text_stream = ssc.textFileStream(stocks_file_path)

        text_stream.pprint()

        ssc.start()
        ssc.awaitTermination()
    # read_from_socket()
    read_from_file()