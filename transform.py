from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import MapType, StringType
import pandas as pd 
import time


class Transform:
    def transform_data(spark,file):
        print("\tWelcome to Transform Project Airport Batch Processing ! ")
        print("-----------------------------------------------------------------")

        df = spark.read.csv(file, header = True,inferSchema = True,mode ="DROPMALFORMED" )
        df.printSchema()
        
        processed_at = time.strftime("%Y-%m-%d %H:%M:%S")
        customer_data = df.select('index','header','name','country','verify','time','rating')
        customer_data = customer_data.withColumn("header",regexp_replace("header","â€™", "'")) \
            .withColumn("header",regexp_replace("header",";", ",")) \
            .withColumn('name',trim(col('name'))) \
            .withColumn('country',trim(col('country'))) \
            .withColumn('time',trim(col('time'))) \
            .withColumn('processed_at',lit(processed_at))
        print("Schema customer table !")
        customer_data.printSchema()
        print("Customer table !")
        customer_data.show(10)
        print("Transform customer table is success !")
        print("-"*40)

        
        
        day_flight = df.select('index','time')
        day_flight = day_flight.withColumn('day_flight_text',split(col('time'),' ')[0]) \
            .withColumn('month_flight_text',split(col('time'),' ')[1]) \
            .withColumn('year_flight',split(col('time'),' ')[2].cast('int'))
        day_flight = day_flight.withColumn('day_flight',when(length(col('day_flight_text')) == 4 \
            , substring(col('day_flight_text'),0,2).cast('int')) \
                .otherwise(substring(col('day_flight_text'),0,1)).cast('int'))
        day_flight = day_flight.withColumn("month_flight", \
            when(col('month_flight_text') == "Jan",1) \
            .when(col('month_flight_text') == "January",1) \
            .when(col('month_flight_text') == "February",2) \
            .when(col('month_flight_text') == "March",3) \
            .when(col('month_flight_text') == "April",4) \
            .when(col('month_flight_text') == "May",5) \
            .when(col('month_flight_text') == "June",6) \
            .when(col('month_flight_text') == "July",7) \
            .when(col('month_flight_text') == "August",8) \
            .when(col('month_flight_text') == "September",9) \
            .when(col('month_flight_text') == "October",10) \
            .when(col('month_flight_text') == "November",11) \
            .when(col('month_flight_text') == "December",12))
        print("Schema day flight table !")
        day_flight.printSchema()
        print("Comment table")
        day_flight.show(10)
        print("Transform day flight table is success ! ")
        print("-"*40)
        
       
        
        review_value_data = df.select('index',split(col('review_value'),",").alias('review_value'))
        review_value_data = review_value_data.select('index','review_value',size(col('review_value')).alias('length'))
        review_value_data = review_value_data \
            .withColumn("Aircraft",when(col('length') == 5,col('review_value').getItem(0)).otherwise(None)) \
            .withColumn("Type_Travel",when(col('length') == 5,col('review_value').getItem(1))\
                .when(col('length') == 4,col('review_value').getItem(0)).otherwise(None)) \
            .withColumn("Seat_Type",when(col('length') == 5,col('review_value').getItem(2)) \
                .when(col('length') == 4,col('review_value').getItem(1)).otherwise(col('review_value').getItem(0))) \
            .withColumn("route",when(col('length') == 5,col('review_value').getItem(3)) \
                .when(col('length') == 4,col('review_value').getItem(2)).otherwise(None))  \
            .withColumn("dateflown",when(col('length') == 5,col('review_value').getItem(4)) \
                .when(col('length') == 4,col('review_value').getItem(3)).otherwise(None))  \
            .drop(col('review_value')).drop(col('length')) 

        review_value_data = review_value_data\
            .withColumn("Aircraft",trim(regexp_replace("Aircraft","\[|\]|\'|\"", ''))) \
            .withColumn("Type_Travel",trim(regexp_replace("Type_Travel","\[|\]|\'|\"", ''))) \
            .withColumn("dateflown",trim(regexp_replace("dateflown","\[|\]|\'|\"", ''))) \
            .withColumn("Seat_Type",trim(regexp_replace("Seat_Type","\[|\]|\'|\"", ''))) \
            .withColumn("route",trim(regexp_replace("route","'", ''))) \
            .withColumn("route",regexp_replace("route"," to ",',')) \
            .withColumn("from",split(col("route"),",")[0]) \
            .withColumn("to",split(col("route"),",")[1]) \
            .drop(col('route'))
        
        print("Schema review value table")
        review_value_data.printSchema()
        
        print("Review value table !")
        review_value_data.show(10,False) 
        print("Transform review value table is success ! ")
        print("-"*40)
        comment_data = df.select('index','rating','comment')
        comment_data = comment_data.withColumn('comment',trim(regexp_replace('comment',"\|",'')))
        print("Schema comment table !")
        comment_data.printSchema()
        print("Comment table")
        comment_data.show(10)
        print("Transform comment table is success ! ")
        print("-"*40)
        return customer_data,day_flight,review_value_data,comment_data

 