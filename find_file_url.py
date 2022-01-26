import pandas as pd
import pytz
from io import BytesIO
import requests
import hashlib
import json
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps
from datetime import datetime, timedelta

import PIL.Image
import PIL.ImageFile
PIL.ImageFile.LOAD_TRUNCATED_IMAGES = True


client = MongoClient('mongodb://localhost:27017')
db = client['cameraTrap_prod']
projects = db['Projects']
# Annotations 為目前版本
a = db['Annotations']
f = db['Files']
e = db['ExchangeableImageFiles']
s = db['Species']
d = db['DataFields']

# TODO: 要先處理好重複annotation的問題，並重新產出一份find_file_url
df = pd.read_csv('/Users/taibif/Documents/01-camera-trap/find_file_url.csv')

df['file_url'] = ''
df['exif'] = ''
df['filename'] = df['source_data'].apply(lambda x: json.loads(x).get('annotation').get('filename'))
df['time'] = df['source_data'].apply(lambda x: json.loads(x).get('annotation').get('time')['$date'])
df['time'] = df['time'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e3))
df['oid'] = df['source_data'].apply(lambda x: json.loads(x).get('annotation').get('_id'))

for i in df.index:
    # a.find({'_id': ObjectId('')})
    if i % 1000 == 0:
        print(i)
    many = a.find({'filename': df['filename'][i], 'time': datetime.datetime.fromtimestamp(
        df['time'][i]/1e3)+timedelta(hours=-8)})
    file_list = [str(k['file']) for k in many if k.get('file')]
    # try to get image
    for fid in file_list:
        url = f'https://camera-trap-api.s3.ap-northeast-1.amazonaws.com/annotation-images/{fid}.jpg'
        response = requests.get(url)
        if response.status_code == 200:  # got image
            file_url = f"{fid}.jpg"  # file object id
            # get exif
            current_file = f.find_one({'_id': ObjectId(fid)})
            if current_file:
                exif_id = current_file.get('exif', '')
                if exif_id:
                    current_exif = e.find_one({'_id': ObjectId(exif_id)})
                    if current_exif:
                        exif = json.loads(dumps(current_exif.get('rawData', '')))
            break
        else:
            url = f'https://d3gg2vsgjlos1e.cloudfront.net/annotation-images/{fid}.jpg'
            response = requests.get(url)
            if response.status_code == 200:  # got image
                file_url = f"{fid}.jpg"  # file object idexcept:
                # get exif
                current_file = f.find_one({'_id': ObjectId(fid)})
                if current_file:
                    exif_id = current_file.get('exif', '')
                    if exif_id:
                        current_exif = e.find_one({'_id': ObjectId(exif_id)})
                        if current_exif:
                            exif = dumps(current_exif.get('rawData', ''))
                break
    df.loc[i, 'file_url'] = file_url
    df.loc[i, 'exif'] = exif


df = df.rename(columns={"image_id": "id"})

# 先存進去之後再一次更新
df[['id', 'file_url', 'exif']].to_csv('/Users/taibif/Documents/01-camera-trap/found_file_url_exif.csv', index=False)
