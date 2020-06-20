import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
# from custom.functions import InvokeModel
from iotfunctions.metadata import EntityType
from scripts.simple_mfg_entities import Turbines
from iotfunctions.db import Database
from iotfunctions.base import BaseTransformer
#from iotfunctions.bif import EntityDataGenerator
#from ai import settings
#import datetime as dt

import sys
import pandas as pd
import numpy as np

import csv
import sqlalchemy


print("start")

if (len(sys.argv) > 0):
    entity_type_name = sys.argv[1]
    input_file = sys.argv[2]
    print("entity_name %s" %entity_type_name)
    print("input_file %s" % input_file)
else:
    print("Please provide path to csv file as script argument")
    exit()

'''
# Replace with a credentials dictionary or provide a credentials
# Explore > Usage > Watson IOT Platform Analytics > Copy to clipboard
# Past contents in a json file.
'''
print("Read credentials")
with open('../bouygues-beta-credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Create a database object to access Watson IOT Platform Analytics DB.
'''
db = Database(credentials = credentials)
db_schema = 'bluadmin' #  set if you are not using the default

print("Delete existing Entity Type")
db.drop_table(entity_type_name, schema = db_schema)

#print("Unregister EntityType")

print("Create Entity Type")
entity = Turbines(name = entity_type_name,
                db = db,
                db_schema = db_schema,
                description = "Equipment Turbines"
                )

#Register EntityType
entity.register(raise_error=False)

# Check to make sure table was created
print("DB Name %s " % entity_type_name)
print("DB Schema %s " % db_schema)
df = db.read_table(table_name=entity_type_name, schema=db_schema)
print( df.head())
entity.read_meter_data(input_file="None")