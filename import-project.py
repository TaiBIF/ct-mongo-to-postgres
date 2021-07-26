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
out = open('pmap.json', 'w')
pmap = {}
for i in project_list:
    '''dts = (i['startTime']+timedelta(hours=+8))
    dte = (i['startTime']+timedelta(hours=+8))
    #areas = [pa_map[str(x)] for x in i['areas']]
    areas = []
    for x in i['areas']:
        areas.append(pa_map[str(x)])
    #print (','.join(areas))
    p = Project(
        name=i['title'],
        description=i.get('description', ''),
        principal_investigator=i.get('principalInvestigator',''),
        funding_agency=i.get('funder',''),
        start_date=dts,
        end_date=dte,
        note=i.get('note', ''),
        region=','.join(areas),
    )
    p.save()'''
    #print (i['startTime'], dt, dt.strftime('%Y-%m-%d'))
    count += 1
    # 補 source_data
    p = Project.objects.get(id=count)
    print (i['title'], p.name)
    pmap[str(i['_id'])] = p.id
    #p = Project.objects.filter(name=i['title']).first()
    #p.source_data = dumps(i)
    #print (type(json.loads(dumps(i))))
    #print (p.source_data)
    #p.save()
out.write(json.dumps(pmap))
