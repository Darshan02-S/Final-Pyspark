import findspark
findspark.init()
findspark.find()
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_date, date_sub
from time import sleep
import os
spark = SparkSession.builder.appName("final").getOrCreate()
while True:
    def data_entry(filename):
        conn = mysql.connector.connect(host="localhost", user="darshan", password="darshan", database="pyspark")
        mycursor = conn.cursor()
        mycursor.execute("use pyspark")
        mycursor.execute("insert into service1(Filename, Status_of_File) values(%s, %s)", [filename, "completed"])
        conn.commit()


    def check(filename):
        conn = mysql.connector.connect(host="localhost", user="darshan", password="darshan", database="pyspark")
        mycursor = conn.cursor()
        mycursor.execute("use pyspark")
        mycursor.execute("select * from service1")
        res = mycursor.fetchall()
        if (filename, "completed") not in res:
            return True
        else:
            return False

    def service():
        df_1 = spark.read.csv("E:\github\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports\cleansed_data\*.csv",
                              header=True, inferSchema=True)
        df_1 = df_1.orderBy(col("Date").desc())
        df_1 = df_1.na.drop()
        last_14 = df_1.filter((col("Date") < date_sub(current_date(), -14))).select("*")
        last_14 = last_14.groupBy(col("Country_Region")).agg(avg(col("Confirmed")).alias("Confirmed"))
        last_14 = last_14.orderBy(col("Confirmed").asc())
        last_14 = last_14.limit(10)
        last_14.repartition(1).write.mode("overwrite").csv("E:\github\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports\decreasing_covid_cases", header=True)

    try:
        dir_path = "E:\github\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports\cleansed_data"
        res = os.listdir(dir_path)
        op = []
        for i in res:
            if i[-4:] == '.csv':
                op.append(i)
        for i in op:
            if check(i[:-4]):
                data_entry(i[:-4])
                service()
            else:
                pass
    except:
        print("No Folder named cleansed_data")

    sleep(30)