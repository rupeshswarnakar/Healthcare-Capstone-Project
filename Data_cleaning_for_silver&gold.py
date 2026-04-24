import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, lit, isnan, count, trim
from pyspark.sql.functions import lpad, regexp_replace
from pyspark.sql.functions import to_date, year, month, dayofmonth, quarter, date_format, monotonically_increasing_id
from pyspark.sql.types import *
# Initialize Spark & Glue context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# S3 bucket config
BUCKET = "s3://healthcare-insurance-project"
BRONZE = f"{BUCKET}/bronze"
SILVER = f"{BUCKET}/silver"

print("Spark session started successfully!")
print(f"Bronze path: {BRONZE}")
print(f"Silver path: {SILVER}")
# Clean patient_records

print("=" * 50)
print("CLEANING: patient_records")
print("=" * 50)

# Read from Bronze
df_patients = spark.read.option("header", True).csv(
    f"{BRONZE}/patient_records/Patient_records.csv"
)

# Show raw stats
print(f"Raw row count: {df_patients.count()}")
print(f"Raw columns: {df_patients.columns}")
print("\nNull counts before cleaning:")


df_patients.select([
    count(when(col(c).isNull() | (col(c) == "NaN") | (col(c) == ""), c)).alias(c)
    for c in df_patients.columns
]).show()

# Replace nulls with NA
df_patients_clean = df_patients.fillna("NA")

# Drop duplicates
df_patients_clean = df_patients_clean.dropDuplicates()

# Rename columns for consistency
df_patients_clean = df_patients_clean \
    .withColumnRenamed("Patient_id", "patient_id") \
    .withColumnRenamed("Patient_name", "patient_name") \
    .withColumnRenamed("patient_gender", "patient_gender") \
    .withColumnRenamed("patient_birth_date", "patient_birth_date") \
    .withColumnRenamed("patient_phone", "patient_phone") \
    .withColumnRenamed("disease_name", "disease_name") \
    .withColumnRenamed("city", "city") \
    .withColumnRenamed("hospital_id", "hospital_id")

print(f"\nClean row count: {df_patients_clean.count()}")
print("\nSample cleaned data:")
df_patients_clean.show(5)

# Write to Silver as Parquet
df_patients_clean.write.mode("overwrite").parquet(
    f"{SILVER}/patient_records/"
)
print("patient_records written to Silver layer!")
# Clean subscriber 

print("=" * 50)
print("CLEANING: subscriber")
print("=" * 50)

# Read from Bronze
df_subscriber = spark.read.option("header", True).csv(
    f"{BRONZE}/subscriber/subscriber.csv"
)

# Show raw stats
print(f"Raw row count: {df_subscriber.count()}")
print(f"Raw columns: {df_subscriber.columns}")
print("\nNull counts before cleaning:")


df_subscriber.select([
    count(when(col(c).isNull() | (col(c) == "NaN") | (col(c) == ""), c)).alias(c)
    for c in df_subscriber.columns
]).show()

# Fix column name with space: 'sub _id' → 'sub_id'
df_subscriber = df_subscriber.withColumnRenamed("sub _id", "sub_id")

# Rename Zip Code column
df_subscriber = df_subscriber.withColumnRenamed("Zip Code", "zip_code")

# Clean all column names to lowercase
df_subscriber = df_subscriber.toDF(*[c.strip().lower().replace(" ", "_") 
                                      for c in df_subscriber.columns])

# Replace nulls with NA
df_subscriber_clean = df_subscriber.fillna("NA")

# Drop duplicates
df_subscriber_clean = df_subscriber_clean.dropDuplicates()

print(f"\nClean row count: {df_subscriber_clean.count()}")
print("\nCleaned columns:", df_subscriber_clean.columns)
print("\nSample cleaned data:")
df_subscriber_clean.show(5)

# Write to Silver as Parquet
df_subscriber_clean.write.mode("overwrite").parquet(
    f"{SILVER}/subscriber/"
)
print("subscriber written to Silver layer!")
# Clean claims

print("=" * 50)
print("CLEANING: claims")
print("=" * 50)

# Read JSON from Bronze
df_claims = spark.read.option("multiline", True).json(
    f"{BRONZE}/claims/claims.json"
)

# Show raw stats
print(f"Raw row count: {df_claims.count()}")
print(f"Raw columns: {df_claims.columns}")
print("\nNull counts + string 'NaN' counts before cleaning:")


df_claims.select([
    count(when(
        col(c).isNull() | (trim(col(c).cast("string")) == "NaN") | (col(c) == ""),
        c
    )).alias(c)
    for c in df_claims.columns
]).show()

# Replace string "NaN" values with "NA" (specific to claims dataset)
df_claims_clean = df_claims
for c in df_claims_clean.columns:
    df_claims_clean = df_claims_clean.withColumn(
        c,
        when(
            trim(col(c).cast("string")) == "NaN", lit("NA")
        ).otherwise(col(c))
    )

# Replace actual nulls with NA
df_claims_clean = df_claims_clean.fillna("NA")

# Drop duplicates
df_claims_clean = df_claims_clean.dropDuplicates()

# Rename SUB_ID to sub_id for consistency
df_claims_clean = df_claims_clean.withColumnRenamed("SUB_ID", "sub_id")

# Fix claim_amount — cast to proper numeric
df_claims_clean = df_claims_clean.withColumn(
    "claim_amount", col("claim_amount").cast("double")
)

print(f"\nClean row count: {df_claims_clean.count()}")
print("\nSample cleaned data:")
df_claims_clean.show(5)

# Write to Silver as Parquet
df_claims_clean.write.mode("overwrite").parquet(
    f"{SILVER}/claims/"
)
print("claims written to Silver layer!")
# Clean hospital

print("=" * 50)
print("CLEANING: hospital")
print("=" * 50)

# Read from Bronze
df_hospital = spark.read.option("header", True).csv(
    f"{BRONZE}/hospital/hospital.csv"
)

print(f"Raw row count: {df_hospital.count()}")
print(f"Raw columns: {df_hospital.columns}")
print("\nNull counts before cleaning:")


df_hospital.select([
    count(when(col(c).isNull() | (col(c) == "NaN") | (col(c) == ""), c)).alias(c)
    for c in df_hospital.columns
]).show()

# Replace nulls with NA
df_hospital_clean = df_hospital.fillna("NA")

# Drop duplicates
df_hospital_clean = df_hospital_clean.dropDuplicates()

# Standardize column names to lowercase
df_hospital_clean = df_hospital_clean.toDF(
    *[c.strip().lower() for c in df_hospital_clean.columns]
)

print(f"\nClean row count: {df_hospital_clean.count()}")
print("\nSample cleaned data:")
df_hospital_clean.show(5)

# Write to Silver as Parquet
df_hospital_clean.write.mode("overwrite").parquet(
    f"{SILVER}/hospital/"
)
print("hospital written to Silver layer!")
# Clean disease

print("=" * 50)
print("CLEANING: disease")
print("=" * 50)

df_disease = spark.read.option("header", True).csv(
    f"{BRONZE}/disease/disease.csv"
)

print(f"Raw row count: {df_disease.count()}")

# Standardize column names
df_disease_clean = df_disease.toDF(
    *[c.strip().lower() for c in df_disease.columns]
)

# Replace nulls, drop duplicates
df_disease_clean = df_disease_clean.fillna("NA").dropDuplicates()

print(f"Clean row count: {df_disease_clean.count()}")
df_disease_clean.show(5)

df_disease_clean.write.mode("overwrite").parquet(f"{SILVER}/disease/")
print("disease written to Silver layer!")

# ── Clean group ────────────────────────────────

print("=" * 50)
print("CLEANING: group")
print("=" * 50)

df_group = spark.read.option("header", True).csv(
    f"{BRONZE}/group/group.csv"
)

print(f"Raw row count: {df_group.count()}")

# Standardize column names
df_group_clean = df_group.toDF(
    *[c.strip().lower() for c in df_group.columns]
)

# Replace nulls, drop duplicates
df_group_clean = df_group_clean.fillna("NA").dropDuplicates()

print(f"Clean row count: {df_group_clean.count()}")
df_group_clean.show(5)

df_group_clean.write.mode("overwrite").parquet(f"{SILVER}/group/")
print("group written to Silver layer!")

# ── Clean grpsubgrp ────────────────────────────

print("=" * 50)
print("CLEANING: grpsubgrp")
print("=" * 50)

df_grpsubgrp = spark.read.option("header", True).csv(
    f"{BRONZE}/grpsubgrp/grpsubgrp.csv"
)

print(f"Raw row count: {df_grpsubgrp.count()}")

# Standardize column names
df_grpsubgrp_clean = df_grpsubgrp.toDF(
    *[c.strip().lower() for c in df_grpsubgrp.columns]
)

# Replace nulls, drop duplicates
df_grpsubgrp_clean = df_grpsubgrp_clean.fillna("NA").dropDuplicates()

print(f"Clean row count: {df_grpsubgrp_clean.count()}")
df_grpsubgrp_clean.show(5)

df_grpsubgrp_clean.write.mode("overwrite").parquet(f"{SILVER}/grpsubgrp/")
print("grpsubgrp written to Silver layer!")

# ── Clean subgroup ─────────────────────────────

print("=" * 50)
print("CLEANING: subgroup")
print("=" * 50)

df_subgroup = spark.read.option("header", True).csv(
    f"{BRONZE}/subgroup/subgroup.csv"
)

print(f"Raw row count: {df_subgroup.count()}")

# Standardize column names
df_subgroup_clean = df_subgroup.toDF(
    *[c.strip().lower() for c in df_subgroup.columns]
)

# Replace nulls, drop duplicates
df_subgroup_clean = df_subgroup_clean.fillna("NA").dropDuplicates()

print(f"Clean row count: {df_subgroup_clean.count()}")
df_subgroup_clean.show(5)

df_subgroup_clean.write.mode("overwrite").parquet(f"{SILVER}/subgroup/")
print("subgroup written to Silver layer!")
print("=" * 50)
print("GOLD LAYER — Building Star Schema")
print("=" * 50)

GOLD = f"{BUCKET}/gold"

# ── dim_hospital ───────────────────────────────────────
print("\nBuilding: dim_hospital")

df_hospital_silver = spark.read.parquet(f"{SILVER}/hospital/")

dim_hospital = df_hospital_silver.select(
    col("hospital_id").cast("string"),
    col("hospital_name").cast("string"),
    col("city").cast("string"),
    col("state").cast("string"),
    col("country").cast("string")
).dropDuplicates(["hospital_id"])

print(f"dim_hospital row count: {dim_hospital.count()}")
dim_hospital.show(5)
dim_hospital.write.mode("overwrite").parquet(f"{GOLD}/dim_hospital/")
print("dim_hospital written to Gold layer!")

# ── dim_disease ────────────────────────────────────────
print("\nBuilding: dim_disease")

df_disease_silver = spark.read.parquet(f"{SILVER}/disease/")

dim_disease = df_disease_silver.select(
    col("disease_id").cast("integer"),
    col("disease_name").cast("string"),
    col("subgrpid").alias("subgrp_id").cast("string")
).dropDuplicates(["disease_id"])

print(f"dim_disease row count: {dim_disease.count()}")
dim_disease.show(5)
dim_disease.write.mode("overwrite").parquet(f"{GOLD}/dim_disease/")
print("dim_disease written to Gold layer!")
# ── dim_subgroup ───────────────────────────────────────
print("Building: dim_subgroup")

df_subgroup_silver  = spark.read.parquet(f"{SILVER}/subgroup/")
df_grpsubgrp_silver = spark.read.parquet(f"{SILVER}/grpsubgrp/")

dim_subgroup = df_subgroup_silver.join(
    df_grpsubgrp_silver,
    df_subgroup_silver["subgrp_id"] == df_grpsubgrp_silver["subgrp_id"],
    "left"
).select(
    df_subgroup_silver["subgrp_id"].cast("string"),
    df_subgroup_silver["subgrp_name"].cast("string"),
    df_subgroup_silver["monthly_premium"].cast("decimal(10,2)"),
    df_grpsubgrp_silver["grp_id"].cast("string")
).dropDuplicates(["subgrp_id"])

print(f"dim_subgroup row count: {dim_subgroup.count()}")
dim_subgroup.show(10)
dim_subgroup.write.mode("overwrite").parquet(f"{GOLD}/dim_subgroup/")
print("dim_subgroup written to Gold layer!")

# ── dim_group ──────────────────────────────────────────
print("\nBuilding: dim_group")

df_group_silver = spark.read.parquet(f"{SILVER}/group/")

dim_group = df_group_silver.select(
    col("grp_id").cast("string"),
    col("grp_name").cast("string"),
    col("grp_type").cast("string"),
    col("country").cast("string"),
    col("city").cast("string"),
    col("premium_written").cast("decimal(14,2)"),
    col("year").cast("integer")
).dropDuplicates(["grp_id"])

print(f"dim_group row count: {dim_group.count()}")
dim_group.show(5)
dim_group.write.mode("overwrite").parquet(f"{GOLD}/dim_group/")
print("dim_group written to Gold layer!")

# ── dim_subscriber ─────────────────────────────────────
print("\nBuilding: dim_subscriber")

df_subscriber_silver = spark.read.parquet(f"{SILVER}/subscriber/")

dim_subscriber = df_subscriber_silver.select(
    col("sub_id").cast("string"),
    col("first_name").cast("string"),
    col("last_name").cast("string"),
    col("birth_date").cast("date"),
    col("gender").cast("string"),
    col("phone").cast("string"),
    col("street").cast("string"),
    col("city").cast("string"),
    col("country").cast("string"),
    col("zip_code").cast("string"),
    col("subgrp_id").cast("string"),
    col("elig_ind").cast("string"),
    col("eff_date").cast("date"),
    col("term_date").cast("date")
).dropDuplicates(["sub_id"])

print(f"dim_subscriber row count: {dim_subscriber.count()}")
dim_subscriber.show(5)
dim_subscriber.write.mode("overwrite").parquet(f"{GOLD}/dim_subscriber/")
print("dim_subscriber written to Gold layer!")
# ── dim_patient ────────────────────────────────────────
print("Building: dim_patient")

df_patient_silver  = spark.read.parquet(f"{SILVER}/patient_records/")
df_hospital_silver = spark.read.parquet(f"{SILVER}/hospital/")

dim_patient = df_patient_silver.join(
    df_hospital_silver.select("hospital_id", "hospital_name"),
    "hospital_id",
    "left"
).select(
    col("patient_id").cast("string"),
    col("patient_name").cast("string"),
    col("patient_gender").cast("string"),
    col("patient_birth_date").cast("date"),
    col("patient_phone").cast("string"),
    col("disease_name").cast("string"),
    col("city").cast("string"),
    col("hospital_id").cast("string"),
    col("hospital_name").cast("string")
).dropDuplicates(["patient_id"])

print(f"dim_patient row count: {dim_patient.count()}")
dim_patient.show(5)
dim_patient.write.mode("overwrite").parquet(f"{GOLD}/dim_patient/")
print("dim_patient written to Gold layer!")

# ── dim_date ───────────────────────────────────────────
print("\nBuilding: dim_date")

df_claims_silver = spark.read.parquet(f"{SILVER}/claims/")

dim_date = df_claims_silver.select(
    to_date(col("claim_date"), "yyyy-MM-dd").alias("full_date")
).dropDuplicates() \
 .withColumn("day",        dayofmonth(col("full_date"))) \
 .withColumn("month",      month(col("full_date"))) \
 .withColumn("month_name", date_format(col("full_date"), "MMMM")) \
 .withColumn("quarter",    quarter(col("full_date"))) \
 .withColumn("year",       year(col("full_date"))) \
 .withColumn("date_id",    monotonically_increasing_id().cast("integer")) \
 .select("date_id", "full_date", "day", "month", "month_name", "quarter", "year")

print(f"dim_date row count: {dim_date.count()}")
dim_date.show(5)
dim_date.write.mode("overwrite").parquet(f"{GOLD}/dim_date/")
print("dim_date written to Gold layer!")
# ── CELL 13: fact_claims ───────────────────────────────
print("=" * 50)
print("Building: fact_claims")
print("=" * 50)

from pyspark.sql.functions import (
    length, when, concat, lit, substring, to_date
)

# Read Silver claims
df_claims_silver = spark.read.parquet(f"{SILVER}/claims/")

# Read dim_date to get date_id
dim_date_gold = spark.read.parquet(f"{GOLD}/dim_date/")

# ── Fix SUB_ID padding issue ───────────────────────────
df_claims_silver = df_claims_silver.withColumn(
    "sub_id",
    when(
        length(col("sub_id")) == 9,
        concat(lit("SUBID1000"), substring(col("sub_id"), 6, 4))
    ).otherwise(col("sub_id"))
)

# ── Join with dim_date to get date_id ─────────────────
df_claims_with_date = df_claims_silver.withColumn(
    "claim_date_parsed",
    to_date(col("claim_date"), "yyyy-MM-dd")
).join(
    dim_date_gold.select("date_id", "full_date"),
    df_claims_silver["claim_date"] == dim_date_gold["full_date"].cast("string"),
    "left"
)

# ── Read dim_patient and dim_subscriber ────────────────
dim_patient_gold    = spark.read.parquet(f"{GOLD}/dim_patient/")
dim_subscriber_gold = spark.read.parquet(f"{GOLD}/dim_subscriber/")

# ── Build fact_claims ──────────────────────────────────
fact_claims = df_claims_with_date \
    .join(
        dim_patient_gold.select("patient_id", "hospital_id"),
        df_claims_with_date["patient_id"] == dim_patient_gold["patient_id"],
        "left"
    ) \
    .join(
        dim_subscriber_gold.select("sub_id", "subgrp_id"),
        df_claims_with_date["sub_id"] == dim_subscriber_gold["sub_id"],
        "left"
    ) \
    .join(
        spark.read.parquet(f"{GOLD}/dim_subgroup/").select("subgrp_id", "grp_id"),
        "subgrp_id",
        "left"
    ) \
    .select(
        df_claims_with_date["claim_id"].cast("integer"),
        df_claims_with_date["patient_id"].cast("string"),
        df_claims_with_date["sub_id"].cast("string"),
        df_claims_with_date["disease_name"].cast("string"),
        dim_patient_gold["hospital_id"].cast("string"),
        dim_subscriber_gold["subgrp_id"].cast("string"),
        col("grp_id").cast("string"),
        col("date_id").cast("integer"),
        df_claims_with_date["claim_type"].cast("string"),
        df_claims_with_date["claim_amount"].cast("decimal(12,2)"),
        col("Claim_Or_Rejected").alias("claim_or_rejected").cast("string"),
        df_claims_with_date["claim_date"].cast("date")
    )

print(f"fact_claims row count: {fact_claims.count()}")
print("\nSample fact_claims:")
fact_claims.show(5)

# Write to Gold
fact_claims.write.mode("overwrite").parquet(f"{GOLD}/fact_claims/")
print("fact_claims written to Gold layer!")
print("\n GOLD LAYER COMPLETE — Star Schema Built!")
job.commit()