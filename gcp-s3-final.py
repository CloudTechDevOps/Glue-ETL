import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
# JDBC read from MySQL
# -----------------------------
jdbc_url = "jdbc:mysql://130.211.192.99:3306/test"

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "users") \
    .option("user", "root") \
    .option("password", "Cloud@123") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

print("Row count:", df.count())

# -----------------------------
# Write CSV to S3 with fixed filename
# -----------------------------
output_bucket = "glue-gcp-1111111"
temp_prefix = "raw-layer/_tmp/"
final_prefix = "raw-layer/"
final_file_name = "gcp-s3.csv"

# Step 1: Write Spark CSV (part file)
df.coalesce(1) \
  .write.mode("overwrite") \
  .option("header", True) \
  .csv(f"s3://{output_bucket}/{temp_prefix}")

# Step 2: Rename part file
s3 = boto3.client("s3")
response = s3.list_objects_v2(Bucket=output_bucket, Prefix=temp_prefix)

for obj in response.get("Contents", []):
    key = obj["Key"]
    if key.endswith(".csv") and "part-" in key:
        s3.copy_object(
            Bucket=output_bucket,
            CopySource={"Bucket": output_bucket, "Key": key},
            Key=f"{final_prefix}{final_file_name}"
        )
        s3.delete_object(Bucket=output_bucket, Key=key)

print(f"File created: s3://{output_bucket}/{final_prefix}{final_file_name}")

job.commit()
