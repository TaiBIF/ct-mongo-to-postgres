from taicat.models import Project, Contact, ProjectMember

import json

from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps
from datetime import datetime, timedelta


client = MongoClient('mongodb://localhost:27017')
db = client['cameraTrap_prod']
projects = db['Projects']
users = db['Users']

users.find({"_id": ObjectId('6048c62ef2338200265e445c')})

u_map = {}
# import users first
for i in users.find({}):
    name = i['name']
    role = i['permission']
    orcid = i['orcId']
    email = i.get('email', None)
    if role == 'administrator':
        is_system_admin = True
    else:
        is_system_admin = False
    if orcid != '0000-0002-4909-3071':
        new_user = Contact.objects.create(name=name, orcid=orcid, email=email, is_system_admin=is_system_admin)
        id = new_user.id
        u_map[str(i['_id'])] = new_user.id


with open('u_map.json', 'w') as fp:
    json.dump(u_map, fp)

# import project member
p_map = open('./ct-mongo-to-postgres/pmap_data.json', 'r')
p_map = json.loads(p_map.read())

for i in projects.find({}):
    member_list = i['members']
    for j in member_list:
        role = j.get('role', None)
        # old -> new
        # executor -> uploader
        # manager -> project_admin
        # researcher -> uploader
        if str(i['_id']) in p_map and str(j['user']) in u_map:
            member_id = u_map[str(j['user'])]
            project_id = p_map[str(i['_id'])]
            if role == 'manager':
                ProjectMember.objects.create(project_id=project_id, role='project_admin', member_id=member_id)
            else:
                ProjectMember.objects.create(project_id=project_id, role='uploader', member_id=member_id)
