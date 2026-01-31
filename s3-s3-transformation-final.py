import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.functions import col
import boto3
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
# Input CSV
# -----------------------------
input_path = "s3://glue-gcp-1111111111/users/part-00000-a1157521-8514-4f28-898d-ec821c80c7ee-c000.csv"

df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [input_path], "recurse": True},
    transformation_ctx="AmazonS3_node"
)

# -----------------------------
# Initial schema mapping
# -----------------------------
mapped_df = ApplyMapping.apply(
    frame=df,
    mappings=[
        ("id", "string", "id", "string"),
        ("username", "string", "user_name", "string"),
        ("email", "string", "Email", "string"),
        ("age", "string", "age", "string"),
        ("city", "string", "city", "string"),
        ("country", "string", "country", "string"),
        ("status", "string", "status", "string"),
        ("phone", "string", "phone", "string"),
        ("created_date", "string", "created_date", "string"),
        ("last_login", "string", "last_login", "string"),

    ],
    transformation_ctx="ChangeSchema_node"
)

# -----------------------------
# Data Quality check
# -----------------------------
DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0
]
"""
EvaluateDataQuality().process_rows(
    frame=mapped_df,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# -----------------------------
# Advanced Transformations
# -----------------------------
# Convert to Spark DataFrame
df_spark = mapped_df.toDF()

# -----------------------------
# 3. Drop unnecessary columns
# -----------------------------
columns_to_drop = ["created_date", "last_login"]  # drop these two columns
existing_cols_to_drop = [c for c in columns_to_drop if c in df_spark.columns]  # safe drop
df_spark = df_spark.drop(*existing_cols_to_drop)

print("Columns after drop:", df_spark.columns)

# 2. Rename columns
df_spark = df_spark.withColumnRenamed("email", "Email")

# 3. Remove duplicate rows
df_spark = df_spark.dropDuplicates()

# 4. Filter invalid data
df_spark = df_spark.filter(col("id").isNotNull() & col("user_name").isNotNull())

# Convert back to DynamicFrame for Glue sink
final_df = DynamicFrame.fromDF(df_spark, glueContext, "final_df")

# -----------------------------
# Write CSV to S3 with custom filename
# -----------------------------
output_bucket = "glue-gcp-1111111111"
output_prefix_temp = "transformation/"  # temporary folder
output_file_name = "custom_users.csv"

# Step 1: Write single CSV to temporary S3 folder
df_spark.coalesce(1).write.mode("overwrite").option("header", True).csv(f"s3://{output_bucket}/{output_prefix_temp}")

# Step 2: Rename the part file to custom filename using boto3
s3_client = boto3.client("s3")
response = s3_client.list_objects_v2(Bucket=output_bucket, Prefix=output_prefix_temp)
for obj in response.get('Contents', []):
    key = obj['Key']
    if key.endswith(".csv") and "part-" in key:
        copy_source = {'Bucket': output_bucket, 'Key': key}
        s3_client.copy_object(Bucket=output_bucket, CopySource=copy_source, Key=f"{output_prefix_temp}{output_file_name}")
        s3_client.delete_object(Bucket=output_bucket, Key=key)

print(f"File uploaded successfully to s3://{output_bucket}/{output_prefix_temp}{output_file_name}")

job.commit()
