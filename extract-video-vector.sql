unload(
  select s3Key, e.embeddings
  FROM video_embedding
  CROSS JOIN UNNEST(embedding) as t(e)
  where e.embedding_scope='video'
) to 's3://video-embedding-dev/video-vector/'
with (format='PARQUET')