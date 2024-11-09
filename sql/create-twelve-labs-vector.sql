-- Create Athena table twelve_labs_vector
-- It contains embedding ALL vectors (video scope + clip scope) returned from twelve labs
CREATE EXTERNAL TABLE `twelve_labs_vector`(
  `s3key` string, 
  `s3lastmodifiedon` timestamp, 
  `s3etag` string, 
  `s3size` int, 
  `processedon` timestamp, 
  `url` string, 
  `embedding` array<struct<id:string,engine:string,task_status:string,exception:string,embeddings:array<float>,start_offset_sec:float,end_offset_sec:float,embedding_scope:string>>)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://video-embedding-dev/embedding'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='video_embedding', 
  'averageRecordSize'='673', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='4', 
  'recordCount'='4', 
  'sizeKey'='17157', 
  'transient_lastDdlTime'='1730827340', 
  'typeOfData'='file')