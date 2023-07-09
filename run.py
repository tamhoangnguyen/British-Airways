
from pyspark.sql.functions import *
from load import Load_data,Find_word
from pyspark.sql import SparkSession
import findspark
findspark.init()


if __name__ == "__main__":
    file = "D:\My_project\Airport_Project_Batch_Processing\data.csv"
    spark = SparkSession \
        .builder \
        .appName("My_project")\
        .master("local[*]") \
        .getOrCreate()
    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("Error")
    pinelines = Load_data()
    pinelines.loads_to_DB(spark,file)
    comment_data = pinelines.getDatacomment()
    find_keywords = Find_word()
    find_keywords.findword(comment_data,sc)

    
