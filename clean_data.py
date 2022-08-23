import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, IntegerType, DoubleType, StructType
from functools import reduce
import os
import mysql.connector
from pyspark.sql.functions import col, to_timestamp
from time import sleep
spark = SparkSession.builder.appName("final").getOrCreate()

while True:
    def data_entry(filename):
        conn = mysql.connector.connect(host="localhost", user="darshan", password="darshan", database="pyspark")
        mycursor = conn.cursor()
        mycursor.execute("use pyspark")
        mycursor.execute("insert into covid_1(Filename, Status_of_File) values(%s, %s)", [filename, "completed"])
        conn.commit()


    def check(filename):
        conn = mysql.connector.connect(host="localhost", user="darshan", password="darshan", database="pyspark")
        mycursor = conn.cursor()
        mycursor.execute("use pyspark")
        mycursor.execute("select * from covid_1")
        res = mycursor.fetchall()
        if (filename, "completed") not in res:
            return True
        else:
            return False


    def clean_data(final_df):
        df = final_df.drop(
            *['FIPS', 'Admin2', 'Lat', 'Long_', 'Deaths', 'Recovered', 'Incident_Rate', 'Case_Fatality_Ratio', 'Active',
              'Combined_Key'])
        df = df.na.drop()
        df = df.select(col("Province_State"), col("Country_Region"),
                       to_timestamp(col("Last_Update"), "yyyy-MM-dd hh:mm:ss").alias("Date"), col("Confirmed"))
        df.repartition(1).write.mode("overwrite").csv(
            "E:\github\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports\cleansed_data", header=True)


    dir_path = "E:\github\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports\Raw_Data"
    res = os.listdir(dir_path)
    data_df = []
    schema = StructType([
        StructField("FIPS", IntegerType(), True),
        StructField("Admin2", StringType(), True),
        StructField("Province_State", StringType(), True),
        StructField("Country_Region", StringType(), True),
        StructField("Last_Update", StringType(), True),
        StructField("Lat", DoubleType(), True),
        StructField("Long_", DoubleType(), True),
        StructField("Confirmed", IntegerType(), True),
        StructField("Deaths", IntegerType(), True),
        StructField("Recovered", IntegerType(), True),
        StructField("Active", IntegerType(), True),
        StructField("Combined_Key", StringType(), True),
        StructField("Incident_Rate", DoubleType(), True),
        StructField("Case_Fatality_Ratio", DoubleType(), True)
    ])
    for i in res:
        df_1 = spark.read.schema(schema).csv(
            "E:\github\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports\Raw_Data\{}".format(i), header=True,
            inferSchema=True)
        if check(i[:10]):
            data_entry(i[:10])

        else:
            pass
        data_df.append(df_1)

    final_data_df = reduce(DataFrame.unionAll, data_df)
    if len(data_df) >= 1:
        clean_data(final_data_df)
    else:
        pass
    sleep(30)
