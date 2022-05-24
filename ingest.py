import pyspark
from pyspark.sql import SparkSession

class Ingest:
    def __init__(self, spark):
        self.spark = spark
        
    def ingestDataFromJDBC(self, table):
        print('ingesting ' + table +' data ...')

        # อ่านข้อมูลจาก postgreSQL ผ่าน jdbc driver
        spark_df = self.spark.read \
            .format('jdbc') \
            .option('url', 'jdbc:postgresql://localhost:5432/lineman') \
            .option('dbtable', 'public.' + table) \
            .option('user', 'postgres') \
            .option('password', 'admin') \
            .load()
        
        return spark_df

    def ingestDataFromSpark(self, table):
        print('ingesting ' + table + ' data ...')

        # อ่านข้อมูลจาก spark table
        spark_df = self.spark.sql("select * from storeddata." + table + ";")
        return spark_df