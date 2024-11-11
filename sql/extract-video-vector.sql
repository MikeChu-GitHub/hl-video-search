-- Extract video scope vector from twelve labs vector
unload(
  select s3Key, e.embeddings
  FROM twelve_labs_vector
  CROSS JOIN UNNEST(embedding) as t(e)
  where e.embedding_scope='video'
  union
  select s3key, e.embeddings
  FROM fixed_video_twelve_labs_vector 
  CROSS JOIN UNNEST(embedding) as t(e)
  where e.embedding_scope='video'
) to 's3://video-embedding-dev/video-vector/'
with (format='PARQUET')