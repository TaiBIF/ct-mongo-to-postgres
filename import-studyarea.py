from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps
from datetime import datetime, timedelta

from taicat.models import Project, StudyArea

import json

client = MongoClient('mongodb://mongo:27017')
db = client['cameraTrap_prod']
projects = db['Projects']

pmap = open('pmap.json', 'r')
pmap_data = json.loads(pmap.read())
#print (pmap_data)

sa = db['StudyAreas']
sa_map_data = {}
count = 0
for i in sa.find():
    count += 1
    #    print (i['title']['zh-TW'], i.get('parent', '--'), i.get('project', ''))
    '''first time, all study_area
    pid = pmap_data[str(i['project'])]
    s = StudyArea(name=i['title']['zh-TW'], project_id=pid)
    s.save()
    '''
    sa_map_data[str(i['_id'])] = count

print(sa_map_data)


count = 0
for i in sa.find():
    count += 1
    if i.get('parent', ''):
        pid = sa_map_data[str(i['parent'])]
        print (pid)
        a = StudyArea.objects.get(id=count)
        a.parent_id = pid
        a.save()
