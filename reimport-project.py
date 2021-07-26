# Reimport project-related collections to fit new database schema (2021)
# modify code based on moogoo's original code (import-project.py)

from taicat.models import Deployment, Project, StudyArea

import json

from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps
from datetime import datetime, timedelta


client = MongoClient('mongodb://localhost:27017')
db = client['cameraTrap_prod']
projects = db['Projects']

# Project Area map
pa = db['ProjectAreas']
pa_map = {}
for i in pa.find():
    pa_map[str(i['_id'])] = i['title']['zh-TW']

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

project_list = projects.find({"_id":{"$in": include_project}})

# Project
pmap_data = {}
for i in project_list:
    dts = (i['startTime']+timedelta(hours=+8))
    dte = (i['endTime']+timedelta(hours=+8))
    areas = [pa_map[str(x)] for x in i['areas']]
    areas = []
    for x in i['areas']:
        areas.append(pa_map[str(x)])
    p = Project(
        name = i['title'],
        description = i.get('description', ''),
        principal_investigator = i.get('principalInvestigator',''),
        funding_agency = i.get('funder',''),
        start_date = dts,
        end_date = dte,
        note = i.get('note', ''),
        region = ','.join(areas),

        source_data = json.loads(dumps(i)),

        short_title = i.get('shortTitle', ''),
        executive_unit = i.get('executiveUnit', ''),
        code = i.get('code', ''),
        publish_date = (i['publishTime']+timedelta(hours=+8)),
        interpretive_data_license = i.get('interpretiveDataLicense', ''),
        identification_information_license = i.get('identificationInformationLicense', ''),
        video_material_license = i.get('videoMaterialLicense', ''),
        mode = "official"
    )
    p.save()
    pmap_data[str(i['_id'])] = p.id

# out.write(json.dumps(pmap))

# Study area
'''
name = models.CharField(max_length=1000)
parent = models.ForeignKey('self', on_delete=models.CASCADE, null=True, blank=True)
project = models.ForeignKey(Project, on_delete=models.SET_NULL, null=True, related_name='studyareas')
created = models.DateTimeField(auto_now_add=True)
'''

sa = db['StudyAreas']
sa_map_data = {}
for i in sa.find({"project":{"$in": include_project}}):
    project_id = pmap_data[str(i['project'])]
    s = StudyArea(name=i['title']['zh-TW'], project_id=project_id)
    s.save()
    sa_map_data[str(i['_id'])] = s.id


# map sub studyarea
for i in sa.find({"project":{"$in": include_project}}):
    parent_id = i.get('parent', '')
    if parent_id != '':
        pid = sa_map_data[str(i['parent'])]
        current_id = sa_map_data[str(i['_id'])]
        a = StudyArea.objects.get(id=current_id)
        a.parent_id = pid
        a.save()


# deployment
'''
    project = models.ForeignKey(Project, on_delete=models.SET_NULL, null=True)
    #cameraDeploymentBeginDateTime
    #cameraDeploymentEndDateTime
    longitude = models.DecimalField(decimal_places=8, max_digits=11, null=True, blank=True)
    latitude = models.DecimalField(decimal_places=8, max_digits=10, null=True, blank=True)
    altitude = models.SmallIntegerField(null=True, blank=True)
    #deploymentLocationID
    name = models.CharField(max_length=1000)
    #cameraStatus
    camera_status = models.CharField(max_length=4, default='1', choices=CAMERA_STATUS_CHOICES)
    study_areas = models.ManyToManyField(StudyArea, blank=True)
    created = models.DateTimeField(auto_now_add=True)
    source_data = models.JSONField(default=dict, blank=True)

    geodetic_datum = models.CharField(max_length=10, default='TWD97', choices=GEODETIC_DATUM_CHOICES)
    landcover = models.CharField('土地覆蓋類型', max_length=1000, blank=True, null=True)
    vegetation = models.CharField('植被類型', max_length=1000, blank=True, null=True)
'''

c = db['CameraLocations']
c_map_data = {}
for i in c.find({"project":{"$in": include_project}}):
    project_id = pmap_data[str(i['project'])]
    # map study area
    study_area_id = sa_map_data[str(i['studyArea'])]
    d = Deployment(
        name = i['name'],
        longitude = i.get('longitude', None),
        latitude = i.get('latitude', None),
        altitude =  i.get('altitude', None),
        source_data = json.loads(dumps(i)),
        geodetic_datum = i.get('geodeticDatum', ''),
        landcover = i.get('landCoverType', ''),
        vegetation = i.get('vegetation', ''),
        project_id = project_id,
    )
    d.save()
    d.study_areas.set([study_area_id])
    c_map_data[str(i['_id'])] = d.id

