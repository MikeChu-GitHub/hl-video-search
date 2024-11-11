import os
import pandas as pd
from dotenv import load_dotenv
from pymilvus import Collection, MilvusClient

print("Reading video-vector...")
df = pd.read_parquet(os.getenv('VIDEO_VECTOR_PATH'), engine='pyarrow')
vectors = df.set_index('s3key')['embeddings'].to_dict()
print(f"Total vector count = {len(vectors)}")

print("Reading vector-db...")
milvus = MilvusClient(os.getenv('MILVUS_DB_PATH'))
if not milvus.has_collection(collection_name=os.getenv('MILVUS_COLLECTION')):
    print('Building vector DB...')
    milvus.create_collection(collection_name=os.getenv('MILVUS_COLLECTION'), dimension=1024)
    data = [
        {
            'id': idx,
            's3key': s3key,
            'vector': vectors[s3key],
        }
        for idx, s3key in enumerate(vectors.keys())
    ]
    res = milvus.insert(collection_name=os.getenv('MILVUS_COLLECTION'), data=data)
    print(res)

# Semantic search with video
def video_search(embedding):
  rs = milvus.search(
      collection_name=os.getenv('MILVUS_COLLECTION'),
      data = [embedding],
      limit=10,
      output_fields=['s3key']
    )
  return [
      {
          'distance': rec['distance'],
          's3key': rec['entity']['s3key'],
      }
      for rec in rs[0]
  ]

print("Finding neighbors...")
df = pd.DataFrame([
    {
        's3key': s3key,
        'neighbors': video_search(vectors[s3key])
    }
    for s3key in vectors
])

print("Writing result to parquet...")
df.to_parquet(os.getenv('NEIGHBOR_PATH'))
