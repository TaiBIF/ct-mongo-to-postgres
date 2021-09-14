# Reimport project-related collections to fit new database schema (2021)
# modify code based on moogoo's original code (import-image.py)
import hashlib
import django
from taicat.models import Deployment, Project, StudyArea, Image
import json

from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps
from datetime import datetime, timedelta

import PIL.Image
import requests
from io import BytesIO
from django.utils import timezone
import pytz

client = MongoClient('mongodb://localhost:27017')
db = client['cameraTrap_prod']
projects = db['Projects']

# c_map_data.json for cameralocation <-> current deployment id
c_map = open('./ct-mongo-to-postgres/c_map_data.json', 'r')
c_map = json.loads(c_map.read())

# 先以100張測試

# get current annotation
# ar = db['AnnotationRevisions']
# ar_list = ar.find({"isCurrent":False})
# ar_list.distinct('annotation').count() #2124,7873 2643,0721

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

subset_a = a.find({ "project": { "$in": include_project } })
total_len = subset_a.count()
img_map_data = {}

for count in range(35841,total_len):
    print(count)
    i = subset_a[count]
    save_or_not = True
    t8 = i['time']+timedelta(hours=+8)
    img_datetime = timezone.make_aware(t8, pytz.timezone('Asia/Taipei'))
    d_id = c_map[str(i['cameraLocation'])]
    from_mongo = True
    filename=i['filename']
    source_data = {'annotation':json.loads(dumps(i))}
    sid = i.get('species', '')
    if sid:
        species = s.find_one({'_id': ObjectId(sid)})['title']['zh-TW']
    else:
        species = ''
    dic = {'species': species}
    # widgetType: select / text
    fields = i.get('fields', '')
    annotations = []
    for j in fields:    
        current_field = d.find_one({'_id':j['dataField']})
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
                if j.get('value',''):
                    v = j.get('value','')
                    select_id = v.get('selectId','') if v else ''
                    if select_id:
                        current_field_value = d.find_one({"_id":j['dataField'], "options._id": select_id }, {'_id':0,'options.$': 1})
                        if current_field_value:
                            current_field_value = current_field_value['options'][0]['zh-TW']
            else: # if text
                current_field_value = j.get('value','')
                if current_field_value:
                    current_field_value = current_field_value.get('text','')
            dic[current_field_key] = current_field_value
    annotations.append(dic)
    # 一張多物種 filename & time will be the same
    many = a.find({'filename': i.get('filename',''), 'time': i.get('time','')})
    id_list = [str(k['_id']) for k in many]
    if len(id_list) > 1:
        # 如果有已經寫入的annotaion，抓出影像的psql id，update那張影像的annotaion
        for l in id_list:
            if l in img_map_data:
                save_or_not = False
                image_id = img_map_data[l]
                anno = Image.objects.get(id=image_id).annotation
                anno += annotations # append to list
                # write back to db
                obj = Image.objects.get(id=image_id)
                obj.annotation = anno
                obj.save()
                break
    # extension
    extension = filename.split('.')[-1].lower()
    img_hash = ''
    file_url = ''
    exif = ''    
    if fid := i.get('file', ''):
        # 影片
        if extension in ['mp4', 'avi']:
            file_url = f"{fid}.mp4"
        else:
            # 圖片
            url = f'https://camera-trap-api.s3.ap-northeast-1.amazonaws.com/annotation-images/{fid}.jpg'
            response = requests.get(url)
            if response.status_code == 200: # got image
                img_file = PIL.Image.open(BytesIO(response.content)) # open image from remote url
                img_hash = hashlib.md5(img_file.tobytes()).hexdigest() # make hash
            file_url = f"{fid}.jpg"  # file object id
        current_file = f.find_one({'_id': ObjectId(fid)})
        if current_file:
            exif_id = current_file.get('exif', '')
            if exif_id:
                current_exif = e.find_one({'_id': ObjectId(exif_id)})
                if current_exif:
                    exif = json.loads(dumps(current_exif.get('rawData', '')))
    if save_or_not:
        new_img = Image(
            deployment = Deployment.objects.get(id=d_id),
            file_url = file_url,
            filename = filename,
            datetime = img_datetime,
            source_data = source_data,
            annotation = annotations,
            image_hash = img_hash,
            exif = exif,
            from_mongo = from_mongo
        )
        new_img.save()
        img_map_data[str(i['_id'])] = new_img.id


with open('./ct-mongo-to-postgres/img_map_data.json', 'w') as fp:
    json.dump(img_map_data, fp)

