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

img_map_data = {}

count = 0
for i in a.find():
    print(count)
    count += 1
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
                    select_id = j.get('value','').get('selectId','')
                if select_id:
                    current_field_value = d.find_one({"_id":j['dataField'], "options._id": select_id }, {'_id':0,'options.$': 1})
                    if current_field_value:
                        current_field_value = current_field_value['options'][0]['zh-TW']
            else: # if text
                current_field_value = j.get('value','').get('text','')
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
    # if count % 1000 == 0:
    #     print(count)


with open('./ct-mongo-to-postgres/img_map_data.json', 'w') as fp:
    json.dump(img_map_data, fp)

