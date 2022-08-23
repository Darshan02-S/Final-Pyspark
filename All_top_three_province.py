import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from time import sleep
import os
import mysql.connector
spark = SparkSession.builder.appName("final").getOrCreate()
while True:

    def data_entry(filename):
        conn = mysql.connector.connect(host="localhost", user="darshan", password="darshan", database="pyspark")
        mycursor = conn.cursor()
        mycursor.execute("use pyspark")
        mycursor.execute("insert into service3(Filename, Status_of_File) values(%s, %s)", [filename, "completed"])
        conn.commit()


    def check(filename):
        conn = mysql.connector.connect(host="localhost", user="darshan", password="darshan", database="pyspark")
        mycursor = conn.cursor()
        mycursor.execute("use pyspark")
        mycursor.execute("select * from service3")
        res = mycursor.fetchall()
        if (filename, "completed") not in res:
            return True
        else:
            return False



    def service():
        df_1 = spark.read.csv("E:\github\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports\cleansed_data\*.csv",header=True, inferSchema=True)
        df_1 = df_1.na.drop()
        df_1 = df_1.groupBy(col("Country_Region"), col("Province_State")).agg(avg(col("Confirmed")).alias("Confirmed"))
        window = Window.partitionBy(col("Country_Region")).orderBy(col("Confirmed").desc())
        df = df_1.withColumn("row_number", row_number().over(window))
        df = df.filter(col("row_number") <= 3)
        df.repartition(1).write.mode("overwrite").csv("E:\github\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports\All_top_three_province", header=True)


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