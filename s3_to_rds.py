import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read CSV from S3
source_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ","
    },
    connection_options={
        "paths": ["s3://veeranareshitdevopss/clean/part-00000-3d4de7c9-f353-43a1-aebe-499c8f4e2be0-c000.csv"]
    }
)

# Map schema
mapped_df = ApplyMapping.apply(
    frame=source_df,
    mappings=[
        ("Year", "string", "Year", "string"),
        ("Industry_name_NZSIOC", "string", "Industry_name_NZSIOC", "string"),
        ("Units", "string", "Units", "string"),
        ("Variable_code", "string", "Variable_code", "string"),
        ("Variable_name", "string", "Variable_name", "string"),
        ("Variable_category", "string", "Variable_category", "string"),
        ("Value", "string", "Value", "double"),
        ("Industry_code_ANZSIC06", "string", "Industry_code_ANZSIC06", "string")
    ]
)

# Write to MySQL RDS
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=mapped_df,
    catalog_connection="mysql-rds-connection",
    connection_options={
        "database": "glue_test_db",
        "dbtable": "enterprise_survey"
    }
)

job.commit()
