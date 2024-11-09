import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from pymilvus import Collection, MilvusClient
from twelvelabs import TwelveLabs

# Build vector db if it does not exist
st.set_page_config(page_title="HL Video Search PoC", layout="centered")

# Fetch video vectors if needed
if 'vectors' not in st.session_state:
    df = pd.read_parquet(os.getenv('VIDEO_VECTOR_PATH'), engine='pyarrow')
    st.session_state.vectors = df.set_index('s3key')['embeddings'].to_dict()

# Build vector DB if needed
if 'milvus' not in st.session_state:
    st.session_state.milvus = MilvusClient(os.getenv('MILVUS_DB_PATH'))
    if not st.session_state.milvus.has_collection(collection_name=os.getenv('MILVUS_COLLECTION')):
        st.write('Building vector DB...')
        st.session_state.milvus.create_collection(collection_name=os.getenv('MILVUS_COLLECTION'), dimension=1024)
        data = [
            {
                'id': idx,
                's3key': s3key,
                'vector': st.session_state.vectors[s3key],
            }
            for idx, s3key in enumerate(st.session_state.vectors.keys())
        ]
        res = st.session_state.milvus.insert(collection_name=os.getenv('MILVUS_COLLECTION'), data=data)
        st.write(res)
        
# Build Twelve client if needed
if 'twelvelabs' not in st.session_state:
    st.session_state.twelvelabs = TwelveLabs(api_key=os.getenv('TWELVE_LABS_API_KEY'))

# Semantic search with text query
def txt_search(query):
    query_embedding = st.session_state.twelvelabs.embed.create(
      engine_name="Marengo-retrieval-2.6",
      text=query,
      text_truncate='none',
    )
    rs = st.session_state.milvus.search(
      collection_name=os.getenv('MILVUS_COLLECTION'),
      data = [query_embedding.text_embedding.segments[0].embeddings_float],
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

# Semantic search with video
def video_search(s3key):
  if s3key in st.session_state.vectors:
    rs = st.session_state.milvus.search(
        collection_name=os.getenv('MILVUS_COLLECTION'),
        data = [st.session_state.vectors[s3key]],
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
  else:
    return []
        
params = st.query_params    
query = st.text_input("Tell me what you like to watch or enter a s3Key.", value=params.get('s3key', ''), key='text_input')
if query:
  if query.startswith('cms/') and query.endswith('.mp4'):
    videos = video_search(query)
  else:
    videos = txt_search(query)
  if videos:
    for idx, video in enumerate(videos):
      if st.button(f"Video #{idx+1} Score: {video['distance']}"):
        st.write(f"s3key: {video['s3key']}")
        st.markdown(f'<a href="?s3key={video["s3key"]}" target="_self">Similar videos</a>', unsafe_allow_html=True)
        st.video(f"{os.getenv('VIDEO_URL_PREFIX')}{video['s3key']}")
  else:
    st.write("Sorry. Nothing match.")



