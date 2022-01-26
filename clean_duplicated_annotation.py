from datetime import timedelta
import datetime
from pymongo import MongoClient
from operator import ge
import pandas as pd
import json
from bson.objectid import ObjectId
from bson.json_util import dumps
import psycopg2
from django.utils import timezone
import pytz

df = pd.read_csv('/Users/taibif/Documents/01-camera-trap/ct_dup_anno.csv')

df.annotation.unique()
a_unique = pd.DataFrame(df.annotation.unique())
a_unique.to_csv('/Users/taibif/Documents/01-camera-trap/a_unique.csv', header=False, index=False)


def get_species(x):
    tmp = eval(x)
    spe_list = []
    for i in tmp:
        if i.get('species') not in spe_list:
            spe_list += [i.get('species')]
    return spe_list


a_unique['spe_list'] = ''

for j in a_unique.index:
    s = get_species(a_unique[0][j])
    a_unique.loc[j, 'spe_list'] = str(s)

# # convert string to list
# df['annotation'] = df['annotation'].apply(lambda x: eval(x))


# # check if all dictionarires are the same in a list
# df['check_len'] = df['annotation'].apply(lambda x: all(a == x[0] for a in x[1:]))

# df = df[df['check_len']]

# df.to_csv('/Users/taibif/Documents/01-camera-trap/ct_check_anno.csv')

# df['anno_str'] = str(df['annotation'])


# many = a.find({'filename': 'IMG_0122.JPG', 'time': datetime.datetime.fromtimestamp(1455976502000/1e3)+timedelta(hours=-8)})

client = MongoClient('mongodb://localhost:27017')
db = client['cameraTrap_prod']
a = db['Annotations']
s = db['Species']
d = db['DataFields']
e = db['ExchangeableImageFiles']
f = db['Files']

# 有可能會有不同樣區，但是檔名＆拍照日期完全一樣的情形，被誤判成同一筆 → 要抓出來
c = pd.read_csv('/Users/taibif/Documents/01-camera-trap/ct_dup_anno_info.csv')
c = c[['image_id', 'source_data']]


# 先用 filename & time 找出annotation
# 確定deployment是不是一樣 (camera location)
c['d'] = c['source_data'].apply(lambda x: json.loads(x).get('annotation').get('cameraLocation').get('$oid'))

c['d_list'] = ''
for i in c.index:
    if i % 1000 == 0:
        print(i)
    row = c.loc[i]
    source = json.loads(row.source_data)
    filename = source.get('annotation').get('filename')
    time = source.get('annotation').get('time').get('$date')
    time = datetime.datetime.fromtimestamp(time / 1e3)+timedelta(hours=-8)
    many = a.find({'filename': filename, 'time': time})
    d_list = []
    for m in many:
        if str(m.get('cameraLocation')) not in d_list and str(m.get('cameraLocation')) != row.d:
            d_list += [str(m.get('cameraLocation'))]
    if len(d_list) > 0:
        c.loc[i, 'd_list'] = str(d_list)

# c = c[c['d_list']!='']

# c = c.to_csv('/Users/taibif/Documents/01-camera-trap/ct_dup_anno_info_c.csv', index=False)

c = pd.read_csv('/Users/taibif/Documents/01-camera-trap/ct_dup_anno_info_c.csv')
c = c.rename(columns={"d": "deployment"})

c_map = open('/Users/taibif/Documents/GitHub/camera-trap-server-1/ct-mongo-to-postgres/c_map_data.json', 'r')
c_map = json.loads(c_map.read())


# 根據filename & time找出annotation
# update目前的annotation
# 新增忽略的annotation

# c['d_len'] = c['d_list'].apply(lambda x: len(eval(x)))

# df[df['annotation']=='[{"species": "空拍"}, {"species": "空拍"}, {"species": "空拍"}]']

# c['d'] = c['source_data'].apply(lambda x: json.loads(x).get('annotation').get('cameraLocation').get('$oid'))
# c['d_list'] = c.d_list.apply(eval)


# filename = 'J04-11130287_2015-11-13-06-53.MP4'
# time = datetime.datetime.fromtimestamp( 1447368788000/ 1e3)+timedelta(hours=-8)
# many = a.find({'filename': filename, 'time': time})


# c.d_len.value_counts()

# 步驟：
# 根據不只一個camera location的filename & time抓出annotation v
# 新增其他筆資料進資料庫
# （先不考慮file_url，但考慮state:active）
# 再檢查一次annotation的長度
# 補上file_url & exif

# 1. 更新原來那一筆資料進資料庫(find file name+time+camera location)
for k in c.index:
    # for k in range(1,2):
    if k >= 3656:
        print(k)
        row = c.loc[k]
        source = json.loads(row.source_data)
        filename = source.get('annotation').get('filename')
        time = source.get('annotation').get('time').get('$date')
        time = datetime.datetime.fromtimestamp(time / 1e3)+timedelta(hours=-8)
        cameraLocation = ObjectId(source.get('annotation').get('cameraLocation').get('$oid'))
        # d_id = c_map[str(cameraLocation)] # 這項沒有變，因為是根據原本的cameraLocation去找
        many = a.find({'filename': filename, 'time': time, 'cameraLocation': cameraLocation, 'state': 'active'})
        annotations = []
        source_datas = []
        for m in many:
            # print(m)
            # fill annotation
            source_data = {'annotation': json.loads(dumps(m))}
            sid = m.get('species', '')
            if sid:
                species = s.find_one({'_id': ObjectId(sid)})['title']['zh-TW']
            else:
                species = ''
            dic = {'species': species}
            fields = m.get('fields', '')
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
                                current_field_value = d.find_one(
                                    {"_id": j['dataField'],
                                     "options._id": select_id},
                                    {'_id': 0, 'options.$': 1})
                                if current_field_value:
                                    current_field_value = current_field_value['options'][0]['zh-TW']
                    else:  # if text
                        current_field_value = j.get('value', '')
                        if current_field_value:
                            current_field_value = current_field_value.get('text', '')
                    dic[current_field_key] = current_field_value
            annotations.append(dic)
            source_datas.append(source_data)
        # update sql
        connection = psycopg2.connect(user="postgres",
                                      password="example",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="cameratrap")
        cursor = connection.cursor()
        # taicat_image
        # UPDATE taicat_image
        # SET exif='{}', source_data='{}';
        query = f"UPDATE taicat_image SET annotation = '{json.dumps(annotations)}' WHERE id = {row.image_id}"
        cursor.execute(query)
        connection.commit()
        # taicat_image_info
        # query = f"UPDATE taicat_image_info SET source_data = '{json.dumps(source_datas)}' WHERE image_id = {row.image_id}"
        # cursor.execute(query)
        # connection.commit()

# ABOVE DONE

# 2. 新增其他筆資料進資料庫
c = c[c['d_list'].notnull()].reset_index(drop=True)
c['d_list'] = c['d_list'].apply(lambda x: eval(x))
c = c.explode('d_list')
c = c.drop('deployment', axis=1)
c = c.rename(columns={'d_list': 'deployment'})
c = c.reset_index(drop=True)


# 4325393

for k in c.index:  # 86152
    # for k in range(1):
    # if k % 1000 == 0:
    # print(k)
    row = c.loc[k]
    source = json.loads(row.source_data)
    filename = source.get('annotation').get('filename')
    time = source.get('annotation').get('time').get('$date')
    time = datetime.datetime.fromtimestamp(time / 1e3)+timedelta(hours=-8)
    cameraLocation = ObjectId(row.deployment)
    d_id = c_map[str(cameraLocation)]
    many = a.find({'filename': filename, 'time': time, 'cameraLocation': cameraLocation, 'state': 'active'})
    annotations = []
    source_datas = []
    for m in many:
        # print(m)
        source_data = {'annotation': json.loads(dumps(m))}
        # fill other fields
        from_mongo = True
        t8 = m['time']+timedelta(hours=+8)
        img_datetime = timezone.make_aware(t8, pytz.timezone('Asia/Taipei'))
        # fill annotation
        sid = m.get('species', '')
        if sid:
            species = s.find_one({'_id': ObjectId(sid)})['title']['zh-TW']
        else:
            species = ''
        dic = {'species': species}
        fields = m.get('fields', '')
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
                            current_field_value = d.find_one(
                                {"_id": j['dataField'],
                                 "options._id": select_id},
                                {'_id': 0, 'options.$': 1})
                            if current_field_value:
                                current_field_value = current_field_value['options'][0]['zh-TW']
                else:  # if text
                    current_field_value = j.get('value', '')
                    if current_field_value:
                        current_field_value = current_field_value.get('text', '')
                dic[current_field_key] = current_field_value
        annotations.append(dic)
        source_datas.append(source_data)
    # update sql
    # taicat_image
    connection = psycopg2.connect(user="postgres",
                                  password="example",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="cameratrap")
    cursor = connection.cursor()
    query = f"INSERT INTO taicat_image (deployment_id, filename, datetime, annotation, from_mongo, count, species, source_data, sequence_definition, \
        life_stage, sex, remarks, animal_id, created, memo, image_hash, file_path, exif \
        ) VALUES ({d_id}, '{filename}', \
            '{img_datetime}', '{json.dumps(annotations)}', 'true', 1, '', '{{}}', '', '', '','','', CURRENT_TIMESTAMP, '', '', '', '{{}}' ) \
                RETURNING id"
    cursor.execute(query)
    image_id = cursor.fetchone()[0]
    connection.commit()
    # taicat_image_info
    query = f"INSERT INTO taicat_image_info (image_id, source_data) VALUES ({image_id}, '{json.dumps(source_datas)}')"
    cursor.execute(query)
    connection.commit()


# TODO: 要補上source_date & exif & file_url & image_hash?
