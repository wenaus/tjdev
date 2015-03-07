#!/usr/bin/env python
#
# Administer the DynamoDB Data Product Catalog (DPC) prototype
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
                         schema=[HashKey('project')],
                         connection=conn,
                         )
else:
    projecttable = Table('project', connection=conn)


## request table
if 'request' not in tables['TableNames']:
    requesttable = Table.create('request',
                         schema=[HashKey('reqid', data_type=NUMBER)],
                         connection=conn,
                         )
else:
    requesttable = Table('request', connection=conn)

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

# List all tables
tables = conn.list_tables()
print 'tables:', tables
print tasktable.count()
