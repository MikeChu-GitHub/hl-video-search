import boto3
import sys
import re
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import concat, current_timestamp, lit, udf
from pyspark.sql.types import *
from twelvelabs import TwelveLabs

# Input parameters
# --DRY_RUN false
# --EMBEDDING_PATH s3://video-embedding-dev/embedding/
# --TWELVE_LABS_API_KEY XXXXX
# --VIDEO_LIST s3://video-embedding-dev/assets/hl_cms_videos.csv
# --VIDEO_PATH s3://hl-transcoded-videos-prod/cms/
# --VIDEO_URL_PREFIX https://videos.shl-svc.com/
# --additional-python-modules twelvelabs==0.3.1
# --PARTITIONS 16
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TWELVE_LABS_API_KEY', 'VIDEO_LIST', 'VIDEO_PATH', 'VIDEO_URL_PREFIX', 'EMBEDDING_PATH', 'DRY_RUN', 'PARTITIONS'])

# Setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# UDF for video embedding generation 
def generate_embedding(path, file_or_url='file'):
    if args['DRY_RUN'].lower()=='true':
        return [
            {
                'id': None,
                'engine': None,
                'task_status': 'DRY_RUN',
                'exception': None,
                'embeddings': None,
                'start_offset_sec': None,
                'end_offset_sec': None,
                'embedding_scope': None,
            }
        ]
    try:
        twelvelabs_client = TwelveLabs(api_key=args['TWELVE_LABS_API_KEY'])
        params = {
            'engine_name': "Marengo-retrieval-2.6",
            'video_clip_length': 5,
            'video_embedding_scopes': ['clip', 'video'],
        }
        params['video_file' if file_or_url=='file' else 'video_url'] = path
        task = twelvelabs_client.embed.task.create(**params)
        task.wait_for_done()
        task_result = twelvelabs_client.embed.task.retrieve(task.id)
        return [
            {
                'id': task_result.id,
                'engine': task_result.engine_name,
                'task_status': task_result.status,
                'exception': None,
                'embeddings': s.embeddings_float,
                'start_offset_sec': s.start_offset_sec,
                'end_offset_sec': s.end_offset_sec,
                'embedding_scope': s.embedding_scope,
            }
            for s in task_result.video_embedding.segments
        ] if task_result.video_embedding and task_result.video_embedding.segments else [
            {
                'id': task_result.id,
                'engine': task_result.engine_name,
                'task_status': task_result.status,
                'exception': None,
                'embeddings': None,
                'start_offset_sec': None,
                'end_offset_sec': None,
                'embedding_scope': None,
            }
        ]
    except Exception as e:
        return [
            {
                'id': None,
                'engine': None,
                'task_status': 'Fail',
                'exception': str(e),
                'embeddings': None,
                'start_offset_sec': None,
                'end_offset_sec': None,
                'embedding_scope': None,
            }
        ]

generate_embedding_udf = udf(
    generate_embedding, 
    ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("engine", StringType(), True),
        StructField("task_status", StringType(), False),
        StructField("exception", StringType(), True),
        StructField("embeddings", ArrayType(FloatType(), True)),
        StructField("start_offset_sec", FloatType(), True),
        StructField("end_offset_sec", FloatType(), True),
        StructField("embedding_scope", StringType(), True)                 
    ])))

# Fetch video list from S3
s3_client = boto3.client('s3')
paginator = s3_client.get_paginator('list_objects_v2')
match = re.match(r's3://([^/]+)/(.+)', args['VIDEO_PATH'])
video_bucket = match.group(1)
video_prefix = match.group(2)
pages = paginator.paginate(Bucket=video_bucket, Prefix=video_prefix)
videos = [
    {
        's3Key': obj['Key'],
        's3LastModifiedOn': obj['LastModified'],
        's3ETag': obj['ETag'],
        's3Size': obj['Size'],
    }
    for page in pages for obj in page.get('Contents', []) if obj['Key'].endswith('.mp4')
]

# Convert video list to dataframe 
schema = StructType(
    [
        StructField('s3Key', StringType(), False),
        StructField('s3LastModifiedOn', TimestampType(), False),
        StructField('s3ETag', StringType(), False),
        StructField('s3Size', IntegerType(), False),
    ]
)
df = spark.createDataFrame(videos, schema)

# Filter videos with VIDEO_LIST
# Only videos listed in VIDEO_LIST are allowed to be processed by vendors
filter_df = spark.read.csv(args['VIDEO_LIST'], header=True)
df = df.join(filter_df, df.s3Key==filter_df.path).drop('path')

# Filter processed videos
match = re.match(r's3://([^/]+)/(.+)', args['EMBEDDING_PATH'])
if 'Contents' in s3_client.list_objects(Bucket=match.group(1), Prefix=match.group(2), Delimiter='/', MaxKeys=1):
    processed_df = spark.read.parquet(args['EMBEDDING_PATH'])
    df = df.join(processed_df.select('s3Key').distinct(), on='s3Key', how='anti')
    df = df.repartition(int(args['PARTITIONS']))
new_video_count = df.count()
print(f"new video count: {new_video_count}")

# Extract embedding if new videos are detected
if new_video_count > 0:
    # Prepare dataframe for embedding extraction
    df = df.withColumn('processedOn', current_timestamp())
    df = df.withColumn('url', concat(lit(args['VIDEO_URL_PREFIX']), df.s3Key))
    df = df.withColumn("embedding", generate_embedding_udf(df.url, 'url'))
    df.write.mode('append').parquet(args['EMBEDDING_PATH'])

job.commit()