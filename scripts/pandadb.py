#!/bin/env python
import os, time, traceback
from datetime import datetime, timedelta
import cx_Oracle

import boto.dynamodb2
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, AllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.types import STRING
from boto.dynamodb2.types import STRING_SET

keyid = os.environ['AWS_ACCESS_KEY_ATLAS']
secretkey = os.environ['AWS_SECRET_KEY_ATLAS']
dyndb = DynamoDBConnection(aws_access_key_id=keyid, aws_secret_access_key=secretkey)

usertable = Table('user', connection=dyndb)

## Panda
db = 'ADCR_PANDAMON'
dbuser = 'ATLAS_PANDAMON_READER'
dbpwd = os.environ[dbuser]
connect=cx_Oracle.connect(dbuser,dbpwd,db)
cursor=connect.cursor()

## DEFT
db = 'ADCR_PANDAMON'
dbuser = 'ATLAS_DEFT_R'
dbpwd = os.environ[dbuser]
deftconnect=cx_Oracle.connect(dbuser,dbpwd,db)
deftcursor=deftconnect.cursor()

def fetchdict(localcursor):
    desc = localcursor.description
    rdlist = []
    for r in localcursor:
        rdict = {}
        i = 0
        for col in r:
            nm = desc[i][0]
            if isinstance(col, cx_Oracle.LOB): #IGNORE:E1101   ignore warning in eclipse
                rdict[nm] = col.read()
            else:
                rdict[nm] = col
            i += 1
        rdlist.append(rdict)
    return rdlist

query = "select MAX(PANDAID) from ATLAS_PANDA.JOBSARCHIVED4"
print query
cursor.execute(query)
rows = fetchdict(cursor)
pandaid = rows[0]['MAX(PANDAID)']
print pandaid

#query = "select * from ATLAS_PANDAMETA.USERS where lastmod >= :tstart"
query = "select * from ATLAS_PANDAMETA.USERS where latestjob >= :tstart"
print query
days = 7
t1 = datetime.utcnow() - timedelta(days)
valdict = { 'tstart' : t1 }
cursor.arraysize=10000
cursor.execute(query, valdict)
rows = fetchdict(cursor)
#with usertable.batch_write() as batch:
if True:
    batch = usertable
    userd = {}
    for user in rows:
        print user['NAME'], user['LATESTJOB'], user['JOBID']
        if user['NAME'] in userd: continue
        userd[user['NAME']] = user['DN']

        try:
            item = usertable.get_item(user=user['NAME'],taskname='latest')
            item['latestjobid'] = user['JOBID']
            item['dn'] = user['DN']
            item['latestjob'] = user['LATESTJOB'].isoformat()
            item.save(overwrite=True)
        except boto.dynamodb2.exceptions.ItemNotFound:
            datadict = { 'user' : user['NAME'], 'taskname' : 'latest', 'dn' : user['DN'], 'latestjob' : user['LATESTJOB'].isoformat(), 'latestjobid' : user['JOBID'] }
            print 'New user', user['NAME']
            batch.put_item(data=datadict,overwrite=True)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback)

print len(rows), 'users in last', days, 'days'

query = "select * from ATLAS_DEFT.T_PRODMANAGER_REQUEST"
deftcursor.execute(query)
rows = fetchdict(deftcursor)
for row in rows:
    print row


query = "select * from ATLAS_DEFT.T_PROJECTS"
deftcursor.execute(query)
rows = fetchdict(deftcursor)
for row in rows:
    print row

