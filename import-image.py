from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps
from datetime import datetime, timedelta

from taicat.models import Project, StudyArea, Deployment, Image
from django.utils import timezone
import pytz
import time
import json

client = MongoClient('mongodb://mongo:27017')
db = client['cameraTrap_prod']
rows = db['Annotations']
files = db['Files']
exifs = db['ExchangeableImageFiles']

#d = open('pmap.json', 'r')
#d2 = open('samap.json', 'r')
d3 = open('clmap.json', 'r')

#data = json.loads(d.read())
#data2 = json.loads(d2.read())
data3 = json.loads(d3.read())


count = 0
for i in rows.find():
    count += 1
    #pid = data[str(i['project'])]
    #said = data2[str(i['studyArea'])]
    depid = data3[str(i['cameraLocation'])]

    #print(i['time'], i['time']+timedelta(hours=+8))
    t8 = i['time']+timedelta(hours=+8)
    tx = timezone.make_aware(t8, pytz.timezone('Asia/Taipei'))
    #print (i['time'], tx)
    #print (time.mktime(tx.timetuple()),time.mktime(t8.timetuple()),time.mktime(i['time'].timetuple()) )
    #print (time.mktime(t8.timetuple())-time.mktime(i['time'].timetuple()))
    #break
    img = Image(
        deployment_id=depid,
        filename=i['filename'],
        datetime=tx,
        source_data={'annotation':json.loads(dumps(i))},
    )
    if fid := i.get('file', ''):
        img.file_url = fid
        res_file = files.find_one({'_id': ObjectId(fid)})
        if res_file:
            exif_id = res_file.get('exif', '')
            if exif_id:
                e = exifs.find_one({'_id': ObjectId(exif_id)})
                if e:
                    img.source_data['exif'] = json.loads(dumps(e))

    img.save()

    if count % 1000 == 0:
        print(count)
        #break
print(count)
