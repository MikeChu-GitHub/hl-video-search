-- Remove bad data and merge parquet files
unload (
  select *
  from twelve_labs_vector
  where not s3key in (
    select s3Key 
    from twelve_labs_vector 
    where cardinality(embedding)=1
    and embedding[1].task_status!='Fail' 
    and (embedding[1].embeddings is null)
  )
) to 's3://video-embedding-dev/embedding_backup/rebuild/'
with (format='PARQUET')
