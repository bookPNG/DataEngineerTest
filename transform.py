import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.functions import col, lit, split, concat_ws, when

class Transform:
    def __init__(self, spark):
        self.spark = spark
        
    def staticValueLatest(self, df, table):
        print('transformimng ' + table + ' data ...')

        # เพิ่ม dt column โดยดึงข้อมูลจาก longitude column แล้วแปลงเป็น string
        transform_df = df.withColumn('dt', col('longitude').cast('string'))

        return transform_df

    def timestampFormat(self, df, table):
        print('transformimng ' + table + ' data ...')

        # เพิ่ม dt column โดยดึงข้อมูลจาก order_created_timestamp column แล้วทำการ split ออกจนเหลือเป็น YYYYMMDD
        transform_df = df.withColumn('dt', concat_ws('', split(split(col('order_created_timestamp'), ' ')[0], '-')).cast('string'))
        
        return transform_df
    
    def cookingBin(self, df, table):
        print('transformimng ' + table + ' data ...')

        # เพิ่ม cooking_bin column โดยใช้ข้อมูลจาก estimated_cooking_time column มาทำ condition ในการ fill ข้อมูลลงไปแล้วจะให้แต่ละ row มีค่าตาม condition ที่กำหนด
        transform_df = df.withColumn('cooking_bin', when((df['estimated_cooking_time'] >= 10) & (df['estimated_cooking_time'] <= 40), lit(1))
                                     .when((df['estimated_cooking_time'] >= 41) & (df['estimated_cooking_time'] <= 80), lit(2))
                                     .when((df['estimated_cooking_time'] >= 81) & (df['estimated_cooking_time'] <= 120), lit(3))
                                     .when(df['estimated_cooking_time'] > 120, lit(4)))
        
        return transform_df

    def replaceNullValue(self, df, table):
        print('transformimng ' + table + ' data ...')

        # เพิ่ม discount_no_null column โดยดึงข้อมูลมาจาก discount column ที่ยังมีค่า null อยู่มา fill ใส่ให้เป็น 0
        df2 = df.withColumn('discount_no_null', col('discount'))
        transform_df = df2.na.fill(0, ['discount_no_null'])

        return transform_df