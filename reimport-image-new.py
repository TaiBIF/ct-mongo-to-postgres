# 2022 from mongodb to psql

import pandas as pd
import threading
import pytz
from django.utils import timezone
from io import BytesIO
import requests
import hashlib
import django
import json
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps
from datetime import datetime, timedelta

import PIL.Image
import PIL.ImageFile
PIL.ImageFile.LOAD_TRUNCATED_IMAGES = True

import glob
from dateutil import parser
import numpy as np


client = MongoClient('mongodb://localhost:27017')
db = client['cameraTrap_prod']
projects = db['Projects']

# c_map_data.json for cameralocation <-> current deployment id
c_map = open('/Users/taibif/Documents/GitHub/camera-trap-server-1/ct-mongo-to-postgres/c_map_data.json', 'r')
c_map = json.loads(c_map.read())

key_list = list(c_map.keys())
val_list = list(c_map.values())

# Annotations 為目前版本
a = db['Annotations']
f = db['Files']
e = db['ExchangeableImageFiles']
s = db['Species']
d = db['DataFields']

include_project = [ObjectId('5ce75c2d4d063e3d8279342f'), ObjectId('5ceb8377f974a93509bb457a'),
                   ObjectId('5d2d459c968e42333ce6f015'), ObjectId('5d2ebb415778d9207444101d'),
                   ObjectId('5d2ebb42d861e5e7833adefb'), ObjectId('5d6e1682d48bef0d6819790d'),
                   ObjectId('5d6f2a6cd48bef40d1199705'), ObjectId('5d79f7cbce838b6f2edab41b'),
                   ObjectId('5d96d825ce838b3a7de64931'), ObjectId('5d9d4c00d109e75584da37ab'),
                   ObjectId('5da97133c641733c60b26808'), ObjectId('5da97799c64173dc91b2696e'),
                   ObjectId('5de74aa011bd907ad8fff166'), ObjectId('5de74abc11bd904442fff1b4'),
                   ObjectId('5ce75c2b4d063e53db7933f9'), ObjectId('5ceb7916f974a96b6ebb322b'),
                   ObjectId('5ceb6101f974a9bd5fbb2a23'), ObjectId('5ceb73ccf974a9ddf4bb2fa8'),
                   ObjectId('5ce7ed824d063e6da4794015'), ObjectId('5ceb7d46f974a946efbb3b84'),
                   ObjectId('5ceb8081f974a94c1fbb4383'), ObjectId('5ceb8026f974a9371bbb419d'),
                   ObjectId('5ceb804ff974a9a319bb4277'), ObjectId('5ceb7ec0f974a956f6bb3f39'),
                   ObjectId('5ce75c2c4d063eac2679340b'),
                   ObjectId('5ceb83dff974a96b6bbb4672'), ObjectId('5ceb4c95f974a93e4ebb22a0'),
                   ObjectId('5ceb83b5f974a9c396bb45ed'), ObjectId('5ceb58fbf974a96a90bb26f6'),
                   ObjectId('5ce75c2c4d063e010b79341d'), ObjectId('5ceb7e0df974a9ea6abb3da9'),
                   ObjectId('5ceb49d3f974a9251dbb222d'), ObjectId('5ceb6902f974a9755fbb2d8b'),
                   ObjectId('5ceb831ef974a98e39bb449e'), ObjectId('5ceb57e9f974a92b5bbb2567'),
                   ObjectId('5ceb7c2ff974a98437bb3843'), ObjectId('5ceb83dcf974a9bcadbb465e'),
                   ObjectId('5de74ace11bd90f94ffff2ed'), ObjectId('5de74afc11bd90f8fffff56e')]

subset_a = a.find({"project": {"$in": include_project}, 'state': 'active'}, no_cursor_timeout=True)
total_len = subset_a.count()  # 4246224

mm = [document.get('_id') for document in subset_a]
# 改成先全部存到csv，再來判斷誰是同一組

df = pd.DataFrame()
count = 0
for m_id in mm:
    count += 1
    if count % 100 == 0:
        print(count)
    uuid = ObjectId()
    # i = subset_a[count]
    i = a.find({'_id': m_id})[0]
    t8 = i['time']+timedelta(hours=+8)
    img_datetime = timezone.make_aware(t8, pytz.timezone('Asia/Taipei'))
    d_id = c_map[str(i['cameraLocation'])]
    filename = i['filename']
    fid = i.get('file', '')
    extension = filename.split('.')[-1].lower()
    file_url = f"{fid}.{extension}" if fid else ''
    fields = i.get('fields', '')
    dic = {}
    sid = i.get('species', '')
    if sid:
        species = s.find_one({'_id': ObjectId(sid)})['title']['zh-TW']
        dic['species'] = species
    for j in fields:
        current_field = d.find_one({'_id': j['dataField']})
        if current_field:
            # key
            if current_field['title'].get('en-US', ''):
                current_field_key = current_field['title']['en-US'].lower().replace(" ", "")
            else:
                if current_field['title']['zh-TW'] == '個體ID':
                    current_field_key = 'animal_id'
                else:
                    current_field_key = current_field['title']['zh-TW']
            # value - could be empty
            current_field_value = ''
            if current_field['widgetType'] == 'select':
                if j.get('value', ''):
                    v = j.get('value', '')
                    select_id = v.get('selectId', '') if v else ''
                    if select_id:
                        current_field_value = d.find_one({"_id": j['dataField'], "options._id": select_id}, {'_id': 0, 'options.$': 1})
                        if current_field_value:
                            current_field_value = current_field_value['options'][0]['zh-TW']
            else:  # if text
                current_field_value = j.get('value', '')
                if current_field_value:
                    current_field_value = current_field_value.get('text', '')
            dic[current_field_key] = current_field_value
    df = df.append({'mongo_index': int(count), 'image_uuid': uuid, 'datetime': img_datetime, 'deployment_id': d_id, 'filename': filename,
                    'fid': fid, 'file_url': file_url, 'extension': extension, 'annotation': dic}, ignore_index=True)
    if (count % 100000 == 0 and count != 0) or count == 4246224:
        df.to_csv(f'mongo_{count}.csv', index=False)
        df = pd.DataFrame()

#------ 統一annotation欄位名稱 ------#

files = glob.glob("*.csv")
df = pd.DataFrame()

for i in files:
    print(i)
    tmp = pd.read_csv(i)
    df = df.append(tmp, ignore_index=True)

df.annotation = df['annotation'].apply(eval)

# split dictionary into columns and join to df
# df = df.join(pd.json_normalize(df.annotation))

anno = pd.json_normalize(df.annotation)
anno = anno[['species', 'remarks', 'sex', 'lifestage', 'antler']]
#['species', 'remarks', 'sex', 'lifestage', 'number', '連拍', '同群', 'studyarea', 'cameralocation', 'filename', 'dateandtime', 'antler']
# species, remarks, sex, lifestage, antler 存成個別column，其他統一存在remark2

remove_keys = ['species', 'remarks', 'sex', 'lifestage', 'antler']
df['remarks2'] = df['annotation']
df['remarks2'] = df.annotation.apply(lambda x: {key: v for key, v in x.items() if key not in remove_keys})

join_df = pd.concat([df, anno], axis=1)

# string to datetime
join_df.datetime = join_df['datetime'].apply(parser.parse)


#------ assign same image_uuid for a picture ------#
# datetime, deployment_id, filename
len(join_df[['datetime', 'deployment_id', 'filename']].drop_duplicates()) # 4120242
join_df.loc[join_df[['datetime', 'deployment_id', 'filename']].duplicated(), 'image_uuid'] = None
len(join_df.image_uuid.unique()) # 4120243

# 把image_uuid塞回去

join_df['gpindex'] = join_df.groupby(['datetime', 'deployment_id', 'filename']).ngroup()

uid = join_df[join_df['image_uuid'].notna()][['gpindex','image_uuid']]
uid = uid.rename(columns={'image_uuid': 'uid'})
df_uid = pd.merge(join_df, uid, on='gpindex')

for r in range(0, len(df_uid), 100000):
    start = r
    end = r + 100000
    print(start, end)
    tmp = df_uid[start:end]
    tmp.to_csv(f'm_{start}_{end}.csv', index=False) 

#------ 取得 image info 資料 ------#

files = glob.glob('/Users/taibif/Documents/01-camera-trap/csv_from_mongo/*')

df = pd.DataFrame()
for i in files:
    print(i)
    tmp = pd.read_csv(i)
    df = df.append(tmp, ignore_index=True)

df['oid'] = mm
df = df.replace({np.nan: ''})
df = df[['gpindex', 'mongo_index', 'image_uuid', 'fid', 'oid']]

g_list = list(df.gpindex.unique())
info_list = pd.DataFrame()
for g in range(len(g_list)):
    if g % 1000 == 0:
        print(g)
    i = g_list[g]
    exif = []
    source_data = []
    for iu in df[df['gpindex'] == i].image_uuid:
        if iu:
            image_uuid = iu
            break
    for k in df[df['gpindex'] == i].oid:
        current_source_data = [x for x in a.find({"_id": ObjectId(k)})]
        source_data += current_source_data
    for j in df[df['gpindex'] == i].fid:
        if j:
            exif_id = f.find_one({'_id': ObjectId(j)}).get('exif')
            current_exif = [x for x in e.find({"_id": exif_id})]
            exif += current_exif
    info_list = info_list.append({'image_uuid': iu, 'source_data': source_data, 'exif': exif}, ignore_index=True)
    if (g % 100000 == 0 and g != 0) or g == 4120241:
        info_list.to_csv(f'info_{g}.csv', index=False)
        info_list = pd.DataFrame()



# ------ 存入資料庫 ------ #
# 以下在 python manage.py shell 進行
from django.db import connection  
from taicat.models import Image_info, Image

files = glob.glob('^/Users/taibif/Documents/01-camera-trap/csv_from_mongo/m_*')

for f in files:
    df = pd.read_csv(f)
    for i in df.index:
        if i % 1000 == 0:
            print(f, i)
        row = df.loc[i]
        row = row.replace(np.nan, '')
        f_url = f'{row.fid}.{row.extension}' if row.fid else ''
        img = Image(
            filename=row.filename,
            datetime=parser.parse(row.datetime),
            species=row.species,
            life_stage=row.lifestage,
            sex=row.sex,
            remarks=row.remarks,
            # animal_id = row.animal_id,
            deployment_id=int(row.deployment_id),
            file_url=f_url,
            annotation=eval(row.annotation),
            # memo = row.memo,
            from_mongo=True,
            antler=row.antler,
            image_uuid=row.uid,
            remarks2=eval(row.remarks2)
        )
        img.save()

# ------ 如果備註最後以.0結尾，則移除.0
with connection.cursor() as cursor:
    query = """select id, remarks from taicat_image where remarks like '%.0'"""
    cursor.execute(query)
    data = cursor.fetchall()

df = pd.DataFrame(data)
df = df.rename(columns={0: 'id', 1: 'remarks'})
df.remarks = df.remarks.apply(lambda x: x.replace('.0', ''))

for i in df.index:
    if i % 1000 == 0:
        print(i)
    obj = Image.objects.filter(id=df.loc[i, 'id']).first()
    obj.remarks = df.loc[i, 'remarks']
    obj.save()


files = glob.glob('^/Users/taibif/Documents/01-camera-trap/csv_from_mongo/info_*')

for f in files:
    df = pd.read_csv(f)
    for i in df.index:
        if i % 1000 == 0:
            print(f, i)
        row = df.iloc[i]
        if Image_info.objects.filter(image_uuid=row.image_uuid).exists():
            pass
        else:
            new = Image_info(
                image_uuid=row.image_uuid,
                exif=row.exif,
                source_data=row.source_data
            )
            new.save()

