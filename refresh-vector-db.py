import boto3
import sys
import re
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import concat, current_timestamp, lit, udf
from pyspark.sql.types import *
from pymilvus import Collection, MilvusClient

# Input parameters
# --EMBEDDING_PATH s3://video-embedding-dev/embedding/
# --MILVUS_DB_PATH s3://video-embedding-dev/vector-db/
# --NEIGHBOR_PATH s3://video-embedding-dev/neighbor/
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'MILVUS_DB_PATH', 'COLLECTION', 'NEIGHBOR_PATH'])

# Setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

milvus = MilvusClient(args['MILVUS_DB_PATH'])