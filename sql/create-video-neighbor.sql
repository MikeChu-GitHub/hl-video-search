-- Create Athena table video_neighbor
-- Top X similar videos for each video
CREATE EXTERNAL TABLE `video_neighbor`(
  `s3key` string, 
  `neighbors` array<struct<distance:double,s3key:string>>)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://video-embedding-dev/neighbor'
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
  'transient_lastDdlTime'='1731012228', 
  'typeOfData'='file')