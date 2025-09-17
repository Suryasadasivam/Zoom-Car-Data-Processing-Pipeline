from pyspark.sql.functions import col, to_date, to_timestamp,when, split,datediff, expr
import re
from datetime import datetime,timedelta

dbutils.widgets.text("process_date", "20240911")
process_date=dbutils.widgets.get("process_date")

#yester_date = datetime.strptime(process_date, "%Y-%m-%d").strftime("%Y%m%d")
process_date = (datetime.strptime(process_date, "%Y-%m-%d") - timedelta(days=2)).strftime("%Y%m%d")


file_path = f"file_path _name"

df=spark.read.option("multiline","true").json(file_path)

df_clean=df.dropna(subset=["booking_id", "customer_id", "car_id", "booking_date"])



df_clean = df_clean.filter(to_date(col("booking_date"), "yy-MM-dd").isNotNull())
df_clean=df_clean.filter(col("status").isin(["completed", "cancelled", "pending", "ongoing"]))

df_clean=df_clean.withColumn("start_timestamp",to_timestamp("start_time"))\
                 .withColumn("end_timestamp",to_timestamp("end_time"))

df_clean=df_clean.withColumn("start_date",to_date("start_timestamp"))

df_clean=df_clean.withColumn("start_date",to_date("start_timestamp"))\
                 .withColumn("start_hour",expr("hour(start_timestamp)"))\
                 .withColumn("end_date",to_date("end_timestamp"))\
                 .withColumn("end_hour",expr("hour(end_timestamp)"))

df_clean=df_clean.withColumn("duration_hours",
                             expr("round((unix_timestamp(end_timestamp) - unix_timestamp(start_timestamp)) / 3600, 2)"))

df_clean.write.format("delta").mode("overwrite").saveAsTable("zoomcar.carbooking.staging_bookings_delta")