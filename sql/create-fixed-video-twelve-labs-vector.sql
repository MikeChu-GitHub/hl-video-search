CREATE EXTERNAL TABLE `fixed_video_twelve_labs_vector`(
  `s3key` string, 
  `embedding` array<struct<id:string,engine:string,task_status:string,exception:string,embeddings:array<double>,start_offset_sec:double,end_offset_sec:double,embedding_scope:string>>)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://video-embedding-dev/fixed-video-embedding/'
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
  'transient_lastDdlTime'='1731166763', 
  'typeOfData'='file')