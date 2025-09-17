# Databricks Notebook: merge_data

from delta.tables import DeltaTable

# ----------------------------
# MERGE BOOKINGS
# ----------------------------

spark.sql("USE CATALOG zoomcar")
spark.sql("USE SCHEMA carbooking")

target_bookings = "target_bookings_delta"
if not spark.catalog.tableExists(f"zoomcar.carbooking.{target_bookings}"):
    spark.sql(f"CREATE TABLE zoomcar.carbooking.{target_bookings} AS SELECT * FROM zoomcar.carbooking.staging_bookings_delta")

else:
    target = DeltaTable.forName(spark, target_bookings)
    staging = spark.table("staging_bookings_delta")

    (target.alias("t")
        .merge(staging.alias("s"), "t.booking_id = s.booking_id")
        .whenMatchedDelete(condition="s.status = 'cancelled'")
        .whenMatchedUpdateAll()  # now this is the last MATCHED clause — valid
        .whenNotMatchedInsertAll()
        .execute())

# ----------------------------
# MERGE CUSTOMERS
# ----------------------------
target_customers = "target_customers_delta"
if not spark.catalog.tableExists(f"zoomcar.carbooking.{target_customers}"):
    spark.sql(f"CREATE TABLE zoomcar.carbooking.{target_customers} AS SELECT * FROM zoomcar.carbooking.staging_customers_delta")

else:
    target = DeltaTable.forName(spark, target_customers)
    staging = spark.table("staging_customers_delta")

    (target.alias("t")
        .merge(staging.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
