"""
03_spark_analysis.py
====================
Spark / Databricks demonstration notebook for SF Housing Project.

Purpose:
  Demonstrate scalable data processing with PySpark on the same
  datasets processed in Snowflake. This serves as a complementary
  analysis showing how the pipeline would scale to larger datasets.

Environment:
  - Databricks Community Edition (free) or Databricks classroom cluster
  - Runtime: DBR 14.x+ with Python 3.10+
  - Or local PySpark: pip install pyspark

Usage on Databricks:
  1. Upload this file as a notebook or convert to .ipynb
  2. Attach to a running cluster
  3. Update S3 paths and run cells sequentially

Usage locally:
  pip install pyspark pandas
  python 03_spark_analysis.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, TimestampType
)

# ──────────────────────────────────────────────
# 1. Initialize Spark Session
# ──────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("SF_Housing_Analysis")
    .config("spark.sql.adaptive.enabled", "true")       # Adaptive Query Execution
    .config("spark.sql.shuffle.partitions", "8")         # Right-size for small data
    .getOrCreate()
)

print(f"Spark version: {spark.version}")
print(f"App name: {spark.sparkContext.appName}")

# ──────────────────────────────────────────────
# 2. Load Data
# ──────────────────────────────────────────────

# Option A: Read from S3 (Databricks with IAM role or keys configured)
# permits_df = spark.read.csv("s3a://sf-housing-project/raw/Building_Permits.csv",
#                              header=True, inferSchema=True)

# Option B: Read from local / DBFS (for demo)
# Upload CSVs to DBFS first: dbutils.fs.cp("file:/tmp/Building_Permits.csv", "dbfs:/data/")

# Option C: Read from local file system
DATA_DIR = "../data/raw"

permits_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("escape", '"')
    .csv(f"{DATA_DIR}/Building_Permits.csv")
)

landuse_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{DATA_DIR}/SF_Land_Use_2023.csv")
)

hpi_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{DATA_DIR}/ATNHPIUS41884Q.csv")
)

census_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{DATA_DIR}/census_acs_bayarea.csv")
)

# ──────────────────────────────────────────────
# 3. Data Profiling
# ──────────────────────────────────────────────

print("=" * 60)
print("DATA PROFILING")
print("=" * 60)

for name, df in [("Permits", permits_df), ("Land Use", landuse_df),
                  ("HPI", hpi_df), ("Census", census_df)]:
    print(f"\n📊 {name}:")
    print(f"   Rows: {df.count():,}")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Partitions: {df.rdd.getNumPartitions()}")

# Show schema for permits
print("\n📋 Permits Schema:")
permits_df.printSchema()


# ──────────────────────────────────────────────
# 4. Spark Transformations
# ──────────────────────────────────────────────

# 4A. Clean permits with Spark
permits_clean = (
    permits_df
    .withColumn("issued_date_ts", F.to_timestamp("Issued Date"))
    .withColumn("filed_date_ts", F.to_timestamp("Filed Date"))
    .filter(F.col("issued_date_ts").isNotNull())
    .withColumn("issued_year", F.year("issued_date_ts"))
    .withColumn("permit_type", F.initcap("Permit Type Definition"))
    .withColumn(
        "processing_days",
        F.datediff("issued_date_ts", "filed_date_ts")
    )
    .filter(
        (F.col("processing_days").isNotNull()) &
        (F.col("processing_days").between(0, 1000))
    )
    .select(
        "Permit Number", "permit_type", "filed_date_ts", "issued_date_ts",
        "issued_year", "processing_days", "Existing Use", "Proposed Use",
        "Estimated Cost", "Neighborhoods - Analysis Boundaries", "Zipcode"
    )
    .withColumnRenamed("Permit Number", "permit_number")
    .withColumnRenamed("Existing Use", "existing_use")
    .withColumnRenamed("Proposed Use", "proposed_use")
    .withColumnRenamed("Estimated Cost", "estimated_cost")
    .withColumnRenamed("Neighborhoods - Analysis Boundaries", "neighborhood")
    .withColumnRenamed("Zipcode", "zipcode")
)

# Cache for reuse (important Spark optimization)
permits_clean.cache()
print(f"\n✅ Cleaned permits: {permits_clean.count():,} rows")


# ──────────────────────────────────────────────
# 5. Analytical Queries with Spark SQL
# ──────────────────────────────────────────────

# Register as temp view for SQL queries
permits_clean.createOrReplaceTempView("permits")
census_df.createOrReplaceTempView("census")

# 5A. Permits trend by year and type
print("\n" + "=" * 60)
print("ANALYSIS 1: Permits Trend by Year")
print("=" * 60)

permits_trend = spark.sql("""
    SELECT
        issued_year,
        permit_type,
        COUNT(*) AS permit_count
    FROM permits
    WHERE issued_year BETWEEN 2000 AND 2024
    GROUP BY issued_year, permit_type
    ORDER BY issued_year, permit_type
""")
permits_trend.show(20)


# 5B. Processing time statistics by permit type
print("\n" + "=" * 60)
print("ANALYSIS 2: Processing Time by Permit Type")
print("=" * 60)

processing_stats = spark.sql("""
    SELECT
        permit_type,
        COUNT(*)                       AS count,
        ROUND(AVG(processing_days), 1) AS avg_days,
        PERCENTILE(processing_days, 0.5)  AS median_days,
        MIN(processing_days)           AS min_days,
        MAX(processing_days)           AS max_days
    FROM permits
    GROUP BY permit_type
    ORDER BY avg_days DESC
""")
processing_stats.show()


# 5C. Neighborhood ranking with window functions
print("\n" + "=" * 60)
print("ANALYSIS 3: Neighborhood Ranking (Window Functions)")
print("=" * 60)

neighborhood_ranked = spark.sql("""
    SELECT
        neighborhood,
        issued_year,
        permit_count,
        RANK() OVER (
            PARTITION BY issued_year
            ORDER BY permit_count DESC
        ) AS rank_in_year,
        permit_count - LAG(permit_count) OVER (
            PARTITION BY neighborhood
            ORDER BY issued_year
        ) AS yoy_change
    FROM (
        SELECT
            neighborhood,
            issued_year,
            COUNT(*) AS permit_count
        FROM permits
        WHERE neighborhood IS NOT NULL
          AND issued_year BETWEEN 2018 AND 2024
        GROUP BY neighborhood, issued_year
    )
    ORDER BY issued_year DESC, rank_in_year
""")
neighborhood_ranked.show(30)


# 5D. Change of Use analysis
print("\n" + "=" * 60)
print("ANALYSIS 4: Top Changes in Building Use")
print("=" * 60)

change_of_use = (
    permits_clean
    .filter(
        (F.col("existing_use").isNotNull()) &
        (F.col("proposed_use").isNotNull()) &
        (F.col("existing_use") != F.col("proposed_use"))
    )
    .withColumn("existing_use", F.initcap("existing_use"))
    .withColumn("proposed_use", F.initcap("proposed_use"))
    .groupBy("existing_use", "proposed_use")
    .count()
    .orderBy(F.desc("count"))
    .limit(15)
)
change_of_use.show()


# ──────────────────────────────────────────────
# 6. Scalability Demonstration
# ──────────────────────────────────────────────

print("\n" + "=" * 60)
print("SCALABILITY NOTES")
print("=" * 60)
print("""
This analysis processes ~200K permit records on a single-node
Spark instance. The same code would scale to millions of records
across multiple cities by:

  1. Reading from a partitioned Parquet dataset on S3
     instead of a single CSV file.

  2. Increasing spark.sql.shuffle.partitions from 8 to 200+
     for larger data volumes.

  3. Using a multi-node Databricks cluster (e.g., 4x m5.xlarge)
     for parallel processing.

  4. Leveraging Delta Lake for ACID transactions and time-travel
     on incrementally updated datasets.

The SQL queries and DataFrame transformations above are already
written in a distributed-first paradigm — no code changes needed
to scale from one city to all 50 states.
""")


# ──────────────────────────────────────────────
# 7. Export results to Parquet (optional)
# ──────────────────────────────────────────────

OUTPUT_DIR = "../data/processed"

permits_trend.coalesce(1).write.mode("overwrite").csv(
    f"{OUTPUT_DIR}/permits_trend_spark", header=True
)

processing_stats.coalesce(1).write.mode("overwrite").csv(
    f"{OUTPUT_DIR}/processing_stats_spark", header=True
)

print("\n✅ Results exported to data/processed/")

# ──────────────────────────────────────────────
# 8. Cleanup
# ──────────────────────────────────────────────
permits_clean.unpersist()
spark.stop()
print("✅ Spark session stopped.")
