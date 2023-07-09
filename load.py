from pyspark.sql.functions import *
from transform_data import Transform
import time
import pandas as pd
import findspark
findspark.init()

class Load_data:
    def __init__(self) -> None:
        self.data = dict()
        self.BootStrapServer = 'localhost:9092'
        self.topic_name = 'Airport_Project'
        self.mysql_host_name = "127.0.0.1"
        self.mysql_port_no = "3306"
        self.mysql_database_name = "britist_airport"
        self.mysql_driver_class = "com.mysql.jdbc.Driver"
        self.mysql_table_name = "Customer_Review"
        self.mysql_user_name = "root"
        self.mysql_password = 't4md3ptr4i'
        self.mysql_jdbc_url = "jdbc:mysql://" + self.mysql_host_name + ":" + self.mysql_port_no + "/" + self.mysql_database_name
    def save_into_mysql(self,data,table,credentials):
        data \
            .write \
            .jdbc( url = self.mysql_jdbc_url,
                table = table,
                mode = 'overwrite',
                properties = credentials)
        print(f"Upload {table} into MySQL was success ! ")
        print("---------------------------------------------------------")
        
    def loads_to_DB(self,spark,file):
        customer_data,day_flight,review_value_data,comment_data = Transform.transform_data(spark,file)
        self.data = {"customer_data": customer_data,
                "day_flight": day_flight,
                "review_data":review_value_data,
                "comment_data":comment_data }
        
        db_credentials = {
                'users' : 'root',
                'password' : self.mysql_password,
                'driver': self.mysql_driver_class
            }
        for name_table, dataset in self.data.items():
            self.save_into_mysql(dataset,name_table,db_credentials)
            time.sleep(1)
    def getDatacomment(self):
        return self.data['comment_data']
        
class Find_word(Load_data):
    def __init__(self) -> None:
        super().__init__()
    def findword(self,df,sc):
        df.printSchema()
        word = ['service','seat','food','beverage','staff']
        df = df.withColumn("comment",regexp_replace(col("comment"),'[.,"]',"")) \
        .withColumn("comment", regexp_replace(col("comment"), "[^a-zA-Z0-9 ]", "")) \
        .withColumn("comment",split(lower(col('comment'))," ")) \
        .filter(col("rating") < 5) \
        .filter(col("verify") == "Trip Verified")
        df.show(5)
        
        for keyword in word:
            df = df.select("comment",array_position(col("comment"),keyword).alias(f"index_{keyword}") \
                ,array_position(col("comment"),keyword + 's').alias(f"index_{keyword}s"))
            df.show(5)
            
            df1 = df.withColumn(f"column_{keyword}",when(col(f'index_{keyword}') > 0 \
                                                        , concat(col('comment').getItem(col(f'index_{keyword}') - 2),lit(" "),
                                                                col('comment').getItem(col(f'index_{keyword}') - 1))).otherwise(0)) \
                        .withColumn(f"column_{keyword}s",when(col(f'index_{keyword}s') > 0 \
                                                        , concat(col('comment').getItem(col(f'index_{keyword}s') - 2),lit(" "),
                                                                col('comment').getItem(col(f'index_{keyword}s') - 1))).otherwise(0))
            df1.show(5)
            # ADD 2 COLUMNS
            result = df1.select('comment',array(col(f"column_{keyword}"),col(f"column_{keyword}s")).alias("array_keykeyword")) 
            result.show(5)
            
            # Collect all result to 1 array
            rows = result.select("array_keykeyword").collect() 
            # Convert array to RDD
            rows = sc.parallelize(rows)
            # Flat row have type pyspark.sql.types.Row to array 2D
            flat_array = rows.flatMap(lambda row: row)
            # Flat array 2D to array 1D
            flat_array = flat_array.flatMap(lambda x: x)
            
            data = flat_array.map(lambda x : (x,1)).reduceByKey(lambda x,y : x + y )
            my_dict = dict()
            for key,value in data.collect():
                my_dict[key] = value
            word = my_dict.keys()
            Number = my_dict.values()
            my_df = pd.DataFrame(list(zip(word,Number)),columns = ['Word',"Number"])
            my_df.to_csv(f"D:\\My_project\\Airport_Project_Batch_Processing\\word\\nums_word_{keyword}_comment.csv",index = False)
            print(f"Save number word {keyword} in comment was success ! ")