from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps
from datetime import datetime, timedelta

from taicat.models import Project, StudyArea, Deployment

import json

client = MongoClient('mongodb://mongo:27017')
db = client['cameraTrap_prod']
rows = db['CameraLocations']

d = open('pmap.json', 'r')
d2 = open('samap.json', 'r')
out = open('clmap.json', 'w')
clmap = {}
data = json.loads(d.read())
data2 = json.loads(d2.read())
#print (data)
count = 0
for i in rows.find():
    count += 1
    pid =str(i['project'])
    if proj_id := data.get(pid, ''):
        #print(proj_id)

        dp = Deployment(
            project_id=proj_id,
            altitude=i.get('altitude',0),
            source_data=json.loads(dumps(i))
        )

        if i['latitude'] > 120:
            print(i['longitude'], i['latitude'], i['_id'], count)
        else:
            dp.longitude=i.get('longitude','')
            dp.latitude=i.get('latitude', '')

        stid = data2[str(i['studyArea'])]
        st = StudyArea.objects.get(id=stid)
        dp.save()
        dp.study_areas.add(st)
        #dp.save()

        #print (str(i['studyarea']))
        clmap[str(i['_id'])] = dp.id
        #print(dp.id)

out.write(json.dumps(clmap))
out.close()
