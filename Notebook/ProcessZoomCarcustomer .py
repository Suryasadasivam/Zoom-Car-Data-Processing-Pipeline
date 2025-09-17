from pyspark.sql.functions import col, to_date, current_date, datediff, regexp_replace,expr
import re
from datetime import datetime,timedelta

dbutils.widgets.text("process_date", "20240912")  # default
process_date = dbutils.widgets.get("process_date")

process_date = (datetime.strptime(process_date, "%Y-%m-%d") - timedelta(days=2)).strftime("%Y%m%d")

# File path
file_path = f"Your file path"
df = spark.read.option("multiline", "true").json(file_path)
df_clean=df.dropna(subset=['customer_id','name','email'])

email_regex=r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
df_clean=df_clean.filter(col("email").rlike(email_regex))

df_clean=df_clean.filter(col("status").isin(["active", "inactive"]))

# Normalize phone number (keep last 10 digits)
df_clean = df_clean.withColumn("phone_number",
                               regexp_replace(col("phone_number"), "[^0-9]", "")) \
                   .withColumn("phone_number", expr("substring(phone_number, -10, 10)"))

# Calculate tenure (days since signup_date)
df_clean = df_clean.withColumn("sign_up_date", to_date("sign_up_date")) \
                   .withColumn("tenure_days", datediff(current_date(), col("sign_up_date")))

# DBTITLE 1,Write to Staging Table
df_clean.write.format("delta").mode("overwrite").saveAsTable("zoomcar.carbooking.staging_customers_delta")