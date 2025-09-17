# 🚗 Zoom Car Data Processing Pipeline with PySpark & Databricks

This project simulates a **real-world data engineering workflow** where **daily data arrives in Azure Storage** and is processed using **PySpark in Databricks**. The pipeline performs **data validation, transformation, and merging** into **Delta Lake tables**, closely mimicking a production ETL setup.

---

## 📁 Data Sources

The pipeline ingests **daily JSON files** representing bookings and customer data:

- `zoom_car_bookings_yyyymmdd.json`
- `zoom_car_customers_yyyymmdd.json`

Each file corresponds to one day of data.

---

## ⚙️ Processing Steps

### 🔹 Notebook 1 – Process Bookings

1. **Read** booking JSON data (parameterized by date).
2. **Validate & Clean**:
   - Remove null records
   - Check date format consistency
   - Enforce valid booking status values
3. **Load** into `staging_bookings_delta` table.

---

### 🔹 Notebook 2 – Process Customers

1. **Read** customer JSON data (parameterized by date).
2. **Validate & Clean**:
   - Check for valid email formats
   - Remove invalid records
   - Standardize customer status values
3. **Load** into `staging_customers_delta` table.

---

### 🔹 Transformations

- **Bookings**:
  - Parse `start_time` and `end_time`
  - Calculate `booking_duration`
  
- **Customers**:
  - Normalize phone numbers
  - Calculate `customer_tenure`

---

### 🔹 Notebook 3 – Merge Data

Merges staged data into **final Delta tables** using the following logic:

- 🔄 **Update**: If record already exists.
- ➕ **Insert**: If record is new.
- ❌ **Delete**: If booking status is `cancelled`.

---

## 🚀 Workflow Automation with Databricks Jobs

The entire pipeline is orchestrated using **Databricks Jobs**, configured to run **daily** with a **date parameter** for dynamic data ingestion.

Execution Flow:

1. `Process Bookings` Notebook  
2. `Process Customers` Notebook  
3. `Merge Data` Notebook  

---

## 💡 Key Learnings

✅ Simulated streaming-like ingestion with daily batch files  
✅ Performed Delta Lake merge operations for data upserts/deletes  
✅ Built modular PySpark notebooks and automated workflows in Databricks  

---

## 📌 Tech Stack

- **Apache Spark (PySpark)**
- **Delta Lake**
- **Azure Data Lake Storage**
- **Databricks Notebooks & Jobs**

---


