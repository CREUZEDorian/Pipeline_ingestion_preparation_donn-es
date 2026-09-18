import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3 (source)
AmazonS3_node1789668577418 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://telco-raw-611284995507-eu-north-1-an/raw/telco_clean.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node1789668577418"
)

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource
where myDataSource.tenure != 0;
'''
SQLQuery_node1789668753394 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": AmazonS3_node1789668577418},
    transformation_ctx="SQLQuery_node1789668753394"
)

df_final = SQLQuery_node1789668753394.toDF().coalesce(1)
SQLQuery_node1789668753394 = DynamicFrame.fromDF(df_final, glueContext, "single_partition")

# Script generated for node Amazon S3 (sink) — écriture du résultat en CSV dans processed/
AmazonS3_sink_node = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1789668753394,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://telco-processed-611284995507-eu-north-1-an/processed/", "partitionKeys": []},
    format_options={"quoteChar": "\"", "separator": ","},
    transformation_ctx="AmazonS3_sink_node"
)

job.commit()

import boto3
s3 = boto3.client("s3")
bucket = "telco-processed-611284995507-eu-north-1-an"
prefix = "processed/"

response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
part_file = [obj["Key"] for obj in response.get("Contents", []) if "part-" in obj["Key"]][0]

s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": part_file}, Key=f"{prefix}telco_filtered.csv")
s3.delete_object(Bucket=bucket, Key=part_file)