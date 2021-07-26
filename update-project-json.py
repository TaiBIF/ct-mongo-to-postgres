from taicat.models import Project

import json

from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps
from datetime import datetime, timedelta


client = MongoClient('mongodb://mongo:27017')
db = client['cameraTrap_prod']
projects = db['Projects']

project_list = projects.find()
results = []
count = 0
#out = open('pmap.json', 'w')
d = open('pmap.json', 'r')
data = json.loads(d.read())

for i in project_list:
    count += 1
    #pmap[str(i['_id'])] = p.id
    p = Project.objects.get(id=count)
    p.source_data = json.loads(dumps(i))
    p.save()
    #print (type(json.loads(dumps(i))))
    #print (p.source_data)
    #p.save()
