import sys
from awsglue.transforms import *
from pyspark.sql.functions import upper
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, regexp_replace, trim, when

# -----------------------------
# Glue job setup
# -----------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------
# 1️⃣ Read messy CSV from S3
# -----------------------------
input_path = "s3://veeranareshitdevopss/messy/enterprise_survey_messy.csv"
df = spark.read.option("header", True).csv(input_path)

print("Original count:", df.count())
df.show(5, truncate=False)

# -----------------------------
# 2️⃣ Layer 1 Transformation: Clean / Standardize
# -----------------------------

# Strip spaces from column names
df = df.toDF(*[c.strip() for c in df.columns])

# Trim whitespace from all string columns
string_cols = [c for c, t in df.dtypes if t == 'string']
for c_name in string_cols:
    df = df.withColumn(c_name, trim(col(c_name)))

# Fill missing Industry_name_NZSIOC with "Unknown"
df = df.withColumn(
    "Industry_name_NZSIOC",
    when(col("Industry_name_NZSIOC").isNull() | (col("Industry_name_NZSIOC") == ""), "Unknown")
    .otherwise(col("Industry_name_NZSIOC"))
)

# Standardize Units column to "Dollars"
df = df.withColumn(
    "Units",
    when(col("Units").isNull() | (col("Units") == ""), "Dollars")
    .otherwise(col("Units"))
)

# Clean Value column: trim, remove commas, convert non-numeric to 0, cast to double
df = df.withColumn("Value", trim(col("Value")))  # remove spaces
df = df.withColumn("Value", regexp_replace(col("Value"), ",", ""))  # remove commas
df = df.withColumn(
    "Value",
    (when(col("Value").rlike("^[0-9.]+$"), col("Value"))
     .otherwise("0.0")).cast("double")  # cast after validation
)

# Standardize Variable_code to uppercase
df = df.withColumn("Variable_code", upper(col("Variable_code")))

# Standardize Year to integer
df = df.withColumn("Year", col("Year").cast("int"))

print("After Layer 1 count:", df.count())
df.show(5, truncate=False)

# -----------------------------
# 3️⃣ Layer 2 Transformation: Enrichment / Derived Columns
# -----------------------------

# Add Adjusted_Value: EXP = negative, REV = positive
df = df.withColumn(
    "Adjusted_Value",
    when(col("Variable_code") == "EXP", col("Value") * -1)  # multiply by -1 instead of unary minus
    .otherwise(col("Value"))
)

# Optional: filter out rows with missing critical data
df = df.filter((col("Year").isNotNull()) & (col("Industry_name_NZSIOC") != "Unknown"))

print("After Layer 2 count:", df.count())
df.show(5, truncate=False)

# -----------------------------
# 4️⃣ Write Clean CSV to S3
# -----------------------------
output_path = "s3://veeranareshitdevopss/clean/"

# coalesce(1) writes a single CSV file (optional)
df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print(f"Cleaned CSV written to: {output_path}")

job.commit()
