from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import shutil
import os

# === CONFIGURATION ===
csv_path = "medical_insurance.csv"
local_dir = r"C:\Users\chani\OneDrive\Documents\PyhtonTraning\delta_time_travel_demo\delta-medical"
delta_path = f"file:///{local_dir.replace(os.sep, '/')}"

# Clean up old delta directory
if os.path.exists(local_dir):
    shutil.rmtree(local_dir)

# === CREATE SPARK SESSION WITH DELTA SUPPORT ===
spark = SparkSession.builder \
    .appName("DeltaMedicalTimeTravel") \
    .master("local[*]") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# === STEP 1: READ CSV FILE ===
df = spark.read.option("header", True).csv(csv_path)
print("📝 Original CSV Data:")
df.show()

# === STEP 2: SAVE AS DELTA TABLE (Version 0) ===
df.write.format("delta").mode("overwrite").save(delta_path)

# === STEP 3: LOAD DELTA TABLE AND PERFORM UPDATE (Version 1) ===
delta_table = DeltaTable.forPath(spark, delta_path)

# 👇 Update example: change region from 'southeast' → 'south'
delta_table.update(
    condition="region = 'southeast'",
    set={"region": "'south'"}
)

# === STEP 4: READ LATEST VERSION (AFTER UPDATE) ===
print("✅ Latest version (after update):")
spark.read.format("delta").load(delta_path).show()

# === STEP 5: TIME TRAVEL TO VERSION 0 ===
print("🕒 Version 0 (original CSV data):")
spark.read.format("delta").option("versionAsOf", 0).load(delta_path).show()

# === STEP 6: VIEW TABLE HISTORY ===
print("📜 Delta Table History:")
delta_table.history().show(truncate=False)

# === DONE ===
spark.stop()
