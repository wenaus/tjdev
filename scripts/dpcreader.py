#!/usr/bin/env python
#
# Read the DynamoDB Data Product Catalog (DPC) prototype
#
import json
import boto.dynamodb2
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, AllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.types import STRING
from boto.dynamodb2.types import STRING_SET

fh = open('/Users/wenaus/Dropbox/work/valet/analtasks.json','r')
taskdata = fh.read()
tasks = json.loads(taskdata)
taskd = {}
for task in tasks:
    taskd[task['taskname']] = task
print tasks[0].keys()
print len(tasks)

# Connect to DynamoDB Local
conn = DynamoDBConnection(
        host='localhost',
            port=8600,
            aws_secret_access_key=None,
            is_secure=False)

tasktable = Table('tasks', connection=conn)

## query
# beginswith  NO: contains
items = tasktable.query_2(taskname__beginswith='mc14_13TeV', project__eq='mc14_13TeV')
for item in items:
    print 'item', item['taskname']

# supported in scan: FILTER_OPERATORS = {'beginswith': 'BEGINS_WITH', 'between': 'BETWEEN', 'contains': 'CONTAINS', 'eq': 'EQ', 'gt': 'GT', 'gte': 'GE', 'in': 'IN', 'lt': 'LT', 'lte': 'LE', 'ncontains': 'NOT_CONTAINS', ...}

items = tasktable.scan(taskname__contains='Graph')
for item in items:
    print 'item filter', item['taskname']

# List all local tables
tables = conn.list_tables()
print tables
