import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, date_format, trim
from awsglue.dynamicframe import DynamicFrame

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
# Read CSV from S3
# -----------------------------
source_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ","
    },
    connection_options={
        "paths": ["s3://glue-gcp-1111111/transformation/custom_users.csv"]
    },
    transformation_ctx="source_df"
)

# -----------------------------
# Convert to Spark DataFrame
# -----------------------------
df = source_df.toDF()

print("Initial row count:", df.count())

# -----------------------------
# Clean & transform data
# -----------------------------

# Trim important columns
df = df.withColumn("id", trim(col("id"))) \
       .withColumn("user_name", trim(col("user_name")))

# Convert ISO timestamp → MySQL DATETIME
df = df.withColumn(
    "created_at",
    date_format(
        to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# Remove invalid rows
df = df.filter(
    col("id").isNotNull() & (col("id") != "") &
    col("user_name").isNotNull() & (col("user_name") != "")
)

# -----------------------------
# Remove duplicate rows (PRIMARY KEY = id)
# -----------------------------
df = df.dropDuplicates(["id"])

print("Row count after deduplication:", df.count())

# -----------------------------
# Convert back to DynamicFrame
# -----------------------------
clean_df = DynamicFrame.fromDF(df, glueContext, "clean_df")

# -----------------------------
# Map schema to match MySQL table
# -----------------------------
final_df = ApplyMapping.apply(
    frame=clean_df,
    mappings=[
        ("id", "string", "id", "string"),
        ("user_name", "string", "user_name", "string"),
        ("Email", "string", "Email", "string"),
        ("age", "string", "age", "string"),
        ("city", "string", "city", "string"),
        ("country", "string", "country", "string"),
        ("status", "string", "status", "string"),
        ("phone", "string", "phone", "string"),
        ("created_at", "string", "created_at", "string")
    ],
    transformation_ctx="final_df"
)

# -----------------------------
# OVERWRITE MySQL table
# (TRUNCATE + INSERT)
# -----------------------------
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=final_df,
    catalog_connection="mysql-rds",   # Glue connection name
    connection_options={
        "database": "test",
        "dbtable": "users",
        "preactions": "TRUNCATE TABLE users;"
    },
    transformation_ctx="write_mysql"
)

print("MySQL table users overwritten successfully")

job.commit()
