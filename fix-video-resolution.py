import ffmpeg
import os
import pandas as pd
import requests
from dotenv import load_dotenv
from twelvelabs import TwelveLabs

def fix_resolution(s3key, url):
    filename = s3key.split('/')[-1]
    res = requests.get(url)
    if os.path.exists(f"{os.getenv('FIXED_VIDEO_PATH')}/tmp/{filename}"):
        os.remove(f"{os.getenv('FIXED_VIDEO_PATH')}/tmp/{filename}")
    with open(f"{os.getenv('FIXED_VIDEO_PATH')}/tmp/{filename}", 'wb') as file:
        file.write(res.content)
    (
        ffmpeg
        .input(f"{os.getenv('FIXED_VIDEO_PATH')}/tmp/{filename}")
        .filter('scale', width=480, height=-2)
        .output(f"{os.getenv('FIXED_VIDEO_PATH')}/{filename}")
        .run()
    )
    return f"{os.getenv('FIXED_VIDEO_PATH')}/{filename}"
    
# UDF for video embedding generation 
def generate_embedding(path, file_or_url='file'):
    try:
        twelvelabs_client = TwelveLabs(api_key=os.getenv('TWELVE_LABS_API_KEY'))
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

print("Reading twelve-labs-vector...")
df = pd.read_parquet(os.getenv('TWELVE_LABS_VECTOR_PATH'), engine='pyarrow')
df = df.explode('embedding')
df = df[df.embedding.apply(lambda e: e['task_status']!='ready')]
df = df[['s3key', 'url']]
videos = df.set_index('s3key')['url'].to_dict()

print("Fixing video resolution...")
data = [
    {
        's3key': s3key, 
        'embedding': generate_embedding(fix_resolution(s3key, videos[s3key])),
    }
    for s3key in videos
]
df = pd.DataFrame(data)
print("Writing result to parquet...")
df.to_parquet(os.getenv('FIXED_VIDEO_VECTOR_PATH'))