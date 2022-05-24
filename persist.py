import pyspark
from pyspark.sql import SparkSession

class Persist:
    def __init__(self, spark):
        self.spark = spark

    def persistData(self, df, table):
        print('persisting ' + table + ' data ...')

        # ทำการเขียนข้อมูลลงใน spark table โดยเขียนทับจากข้อมูลเดิมที่มีอยู่
        df.write.mode('overwrite').saveAsTable("storeddata." + table)

        # ทำการแสดงข้อมูลใน terminal
        self.spark.sql("select * from storeddata." + table + ";").show()