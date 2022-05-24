import ingest
import transform
import persist

import sys
import pyspark
from pyspark.sql import SparkSession

class Pipeline:

    def runDtColumnPipeline(self):
        print('--------------------running df Column Pipeline----------------------------')

        # เรียกใช้งาน ingest, transform, persist
        ingest_process = ingest.Ingest(self.spark)
        transform_process = transform.Transform(self.spark)
        persist_process = persist.Persist(self.spark)

        restaurant = "restaurant_detail"
        order = "order_detail"

        # ทำการ ETL ข้อมูล (postgreSQL -> transform data -> Hive)
        try :
            df = ingest_process.ingestDataFromJDBC(restaurant)
            transform_df = transform_process.staticValueLatest(df, restaurant)
            persist_process.persistData(transform_df, restaurant)

            df = ingest_process.ingestDataFromJDBC(order)
            transform_df = transform_process.timestampFormat(df, order)
            persist_process.persistData(transform_df, order)
        except Exception as e:
            print(e)
            sys.exit(1)

        return

    def runNewColumnPipeline(self):
        print('--------------------running New Column Pipeline----------------------------')

        # เรียกใช้งาน ingest, transform, persist
        ingest_process = ingest.Ingest(self.spark)
        transform_process = transform.Transform(self.spark)
        persist_process = persist.Persist(self.spark)

        # ทำการ ETL ข้อมูลเดิมที่มีอยู่ใน Hive (Hive -> transform data -> Hive)
        try :
            df = ingest_process.ingestDataFromSpark("restaurant_detail")
            transform_df = transform_process.cookingBin(df, "restaurant_detail")
            persist_process.persistData(transform_df, "restaurant_detail_new")

            df = ingest_process.ingestDataFromSpark("order_detail")
            transform_df = transform_process.replaceNullValue(df, "order_detail")
            persist_process.persistData(transform_df, "order_detail_new")
        except Exception as e:
            print(e)
            sys.exit(1)

    def sqlRequires(self):
        print('getting avg_discount csv file ...')

        # ทำการเรียกข้อมูลโดยใช้ spark sql ดึงข้อมูล average discount ตาม category ต่างๆ
        avg_discount = self.spark.sql("select r.category, avg(discount_no_null) avg_discount "
                            "from storeddata.restaurant_detail_new as r join storeddata.order_detail_new as o on r.id = o.restaurant_id "
                            "group by category;")

        # เขียนไฟล์ csv จากข้อมูลที่ query ออกมาได้
        avg_discount.write.option("header", "true").option("delimiter", ",").mode('overwrite').csv("csv-req\discount")


        print('getting count_cooking_bin csv file ...')

        # ทำการเรียกข้อมูลโดยใช้ spark sql ดึงข้อมูล row count ตามแต่ละ cooking bin
        count_cooking_bin = self.spark.sql("select cooking_bin, count(*) row_count "
                                      "from storeddata.restaurant_detail_new "
                                      "group by cooking_bin;")

        # เขียนไฟล์ csv จากข้อมูลที่ query ออกมาได้
        count_cooking_bin.write.option("header", "true").option("delimiter", ",").mode('overwrite').csv("csv-req\cooking")

    def createSparkSession(self):
        # สร้าง session สำหรับการเชื่อมต่อกับ spark โดยจะมี hive support และ jdbc driver เพิ่มเติมมาด้วย
        self.spark = SparkSession.builder.appName("spark app")\
            .config("spark.sql.catalogImplementation", "hive") \
            .config('spark.driver.extraClassPath', 'postgresql-42.3.5.jar') \
            .enableHiveSupport().getOrCreate()

    def createHiveTable(self):
        # สร้างฐานข้อมูลบน spark
        self.spark.sql("create database if not exists storeddata")

        # สร้างตาราง restaurant_detail, order_detail, restaurant_detail_new และ order_detail_new
        self.spark.sql("create table if not exists storeddata.restaurant_detail ("
                       "id character(255),restaurant_name character(255),"
                       "category character(255),estimated_cooking_time real,"
                       "latitude real,longitude real,dt character(255));")

        self.spark.sql("create table if not exists storeddata.order_detail ("
                       "order_created_timestamp timestamp,status character(255),"
                       "price integer,discount real,id character(255),driver_id character(255),"
                       "user_id character(255),restaurant_id character(255),dt character(255));")

        self.spark.sql("create table if not exists storeddata.restaurant_detail_new ("
                       "id character(255),restaurant_name character(255),"
                       "category character(255),estimated_cooking_time real,"
                       "latitude real,longitude real,dt character(255), cooking_bin real);")

        self.spark.sql("create table if not exists storeddata.order_detail_new ("
                       "order_created_timestamp timestamp,status character(255),"
                       "price integer,discount real,id character(255),driver_id character(255),"
                       "user_id character(255),restaurant_id character(255),dt character(255), discount_no_null real);")

        self.spark.sql("SET datestyle = dmy;")

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.createSparkSession()
    pipeline.createHiveTable()
    pipeline.runDtColumnPipeline()
    pipeline.runNewColumnPipeline()
    pipeline.sqlRequires()