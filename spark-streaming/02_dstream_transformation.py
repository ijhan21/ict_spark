from collections import namedtuple
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# 3.2.4로 하다가 자꾸 팅겨서 3.4.1로 전체 변경해서 해서 해결함

columns = ['Ticker','Date','Open','High','Low','Close','AdjClose','Volume']
Finance = namedtuple("Finance", columns)

if __name__ == '__main__':

    sc = SparkSession.builder\
            .master("local[2]")\
            .appName("Dstream Transformations")\
            .getOrCreate().sparkContext
    
    ssc = StreamingContext(sc, 5)

    # 데이터를 읽음과 동시에 파싱
    def read_finance():

        # 1. map function
        def parse(line):
            arr = line.split(",") # 쉼표 기준 split
            # Finance named tuple 형식으로 리턴
            return Finance(*arr)
        
        return ssc.socketTextStream("localhost", 12345).map(parse)

    # read_finance().pprint()
    finance_stream = read_finance()

    # filtering
    def filter_nvda():
        finance_stream.filter(lambda x : x.Ticker == "NVDA").pprint()

    # Volume이 190,000,000 이상인 데이터만 필터링
    def filter_volume():
        finance_stream.filter(lambda x : int(x.Volume) > 190000000 ).reduceByKey(lambda a, b : a+b).pprint()
    # filter_nvda()
    # filter_volume()

    def group_by_datas_volume():
        finance_stream.map(lambda x: (x.Date, int(x.Volume))).groupByKey().mapValues(sum).pprint()
    # group_by_datas_volume()

    # foreac RDD
    def save_to_json():
        def foreach_func(rdd):
            # RDD가 비어있는 경우에는 아무것도 하지 않기
            # 5초동안 데이터가 들어오지 않으면 마이크로 배치가 비어있기 때문에 비어있는 RDD가 생성될 수 있다.
            if rdd.isEmpty():
                print("RDD is Empty")
                return
            df = rdd.toDF(columns)

            # 저장 경로
            dir_path = "/home/ubuntu/working/spark-examples/spark-streaming/data/stocks/outputs"

            # 경로 내에 몇 개의 파일이 있는지를 구하고, 인덱스를 생성
            n_files = len(os.listdir(dir_path)) # 디렉토리 경로에 파일의 개수 구하기
            path = f"{dir_path}/finance-{n_files}.json"
            df.write.json(path)
            print(f"num-partitions => {df.rdd.getNumPartitions()}")
            print("Write Complete")
        finance_stream.foreachRDD(foreach_func)
    save_to_json()
    ssc.start()
    ssc.awaitTermination()