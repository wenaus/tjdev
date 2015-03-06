#!/usr/bin/env python
#
# Create and load the DynamoDB Data Product Catalog (DPC) prototype
#
import sys, os, time
import json
import boto.dynamodb2
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, AllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.types import STRING
from boto.dynamodb2.types import STRING_SET

intasks = [ '/Users/wenaus/Dropbox/work/valet/analtasks.json',
            '/Users/wenaus/Dropbox/work/valet/prodtasks.json' ]

## Load up test task tables
taskd = {}
for t in intasks:
    fh = open(t,'r')
    taskdata = fh.read()
    tasks = json.loads(taskdata)
    for task in tasks:
        taskd[task['taskname']] = task
    print tasks[0].keys()
    print len(tasks)
    fh.close()

## Connect to DynamoDB
#mode = 'delete'
#mode = 'local'
#mode = 'wenaus'
mode = 'atlas'
if mode == 'local':
    ## Local server must be running
    conn = DynamoDBConnection(host='localhost', port=8600, aws_secret_access_key=None, is_secure=False)
elif mode == 'atlas':
    keyid = os.environ['AWS_ACCESS_KEY_ATLAS']
    secretkey = os.environ['AWS_SECRET_KEY_ATLAS']
    conn = DynamoDBConnection(aws_access_key_id=keyid, aws_secret_access_key=secretkey)
elif mode == 'wenaus':
    keyid = os.environ['AWS_ACCESS_KEY']
    secretkey = os.environ['AWS_SECRET_KEY']
    conn = DynamoDBConnection(aws_access_key_id=keyid, aws_secret_access_key=secretkey)
elif mode == 'delete':
    tables = conn.list_tables()
    for table in tables['TableNames']:
        t = Table(table, connection=conn)
        t.delete()
    sys.exit()

# List all tables
tables = conn.list_tables()
print 'tables:', tables

## task table
if 'task' not in tables['TableNames']:
    tasktable = Table.create('task',
                         schema=[HashKey('project'), RangeKey('taskname'), ],
                         throughput={ 'read': 5, 'write': 5 },
                         indexes=[ AllIndex('EverythingIndex', parts=[ HashKey('project'), RangeKey('taskname'), ]), ],
                         connection=conn,
                         )
else:
    tasktable = Table('task', connection=conn)

## project table
if 'project' not in tables['TableNames']:
    projecttable = Table.create('project',
                         schema=[HashKey('project'), RangeKey('taskname'), ],
                         throughput={ 'read': 5, 'write': 5 },
                         indexes=[ AllIndex('EverythingIndex', parts=[ HashKey('project'), RangeKey('modificationtime'), ]), ],
                         connection=conn,
                         )
else:
    projecttable = Table('project', connection=conn)


## user table
if 'user' not in tables['TableNames']:
    usertable = Table.create('user',
                         schema=[HashKey('user'), RangeKey('taskname'), ],
                         throughput={ 'read': 5, 'write': 5 },
                         indexes=[ AllIndex('EverythingIndex', parts=[ HashKey('user'), RangeKey('modificationtime'), ]), ],
                         connection=conn,
                         )
else:
    usertable = Table('user', connection=conn)


## tag table
if 'tag' not in tables['TableNames']:
    tagtable = Table.create('tag',
                         schema=[HashKey('tag'), RangeKey('taskname'), ],
                         throughput={ 'read': 5, 'write': 5 },
                         indexes=[ AllIndex('EverythingIndex', parts=[ HashKey('tag'), RangeKey('modificationtime'), ]), ],
                         connection=conn,
                         )
else:
    tagtable = Table('tag', connection=conn)

## physicstable
if 'physics' not in tables['TableNames']:
    physicstable = Table.create('physics',
                         schema=[HashKey('physics'), RangeKey('taskname'), ],
                         throughput={ 'read': 5, 'write': 5 },
                         indexes=[ AllIndex('EverythingIndex', parts=[ HashKey('physics'), RangeKey('modificationtime'), ]), ],
                         connection=conn,
                         )
else:
    physicstable = Table('physics', connection=conn)

## populate the tables
idx = 0
with tasktable.batch_write() as batch:
#if True:
    #batch = tasktable
    for t in taskd:
        idx += 1
        if idx > 20000: continue
        task = taskd[t]
        name = task['taskname']
        print len(taskd), idx, name
        fields = name.split('.')
        if len(fields) < 5: continue
        #print name
        if fields[0] == 'user':
            project = 'user:%s' % fields[1]
        else:
            project = 'project:%s' % fields[0]
        stream = fields[2]
        step = fields[3]
        tags = fields[4]
        #tagl = tags.split('_')
        #print "put task", task['taskname']
        datadict = { 'taskname' : name, 'taskid' : task['jeditaskid'], 'project' : project, 'stream' : stream, 'step' : step }
        for t in tags:
            datadict[t] = 1
        batch.put_item(data=datadict,overwrite=True)

print 'task table loading done'

## query
# beginswith  NO: contains
items = tasktable.query_2(taskname__beginswith='mc14_13TeV', project__eq='project:mc14_13TeV')
for item in items:
    print 'item', item['taskname']

# supported in scan: FILTER_OPERATORS = {'beginswith': 'BEGINS_WITH', 'between': 'BETWEEN', 'contains': 'CONTAINS', 'eq': 'EQ', 'gt': 'GT', 'gte': 'GE', 'in': 'IN', 'lt': 'LT', 'lte': 'LE', 'ncontains': 'NOT_CONTAINS', ...}

items = tasktable.scan(taskname__contains='Graph')
for item in items:
    print 'item filter', item['project'], item['taskname']

# List all tables
tables = conn.list_tables()
print 'tables:', tables
print tasktable.count()
