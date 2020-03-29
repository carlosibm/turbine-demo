import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif, metadata
from iotfunctions.bif import EntityDataGenerator
from ai.functions import DimsPreload
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from ai import settings

EngineLogging.configure_console_logging(logging.DEBUG)

'''
# Replace with a credentials dictionary or provide a credentials
# Explore > Usage > Watson IOT Platform Analytics > Copy to clipboard
# Past contents in a json file.
'''

#with open('credentials.json', encoding='utf-8') as F:
db_schema = 'bluadmin' #  set if you are not using the default
with open('credentials_Monitor-Demo.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
#db_schema = 'dash100462'  # replace if you are not using the default schema
#with open('credentials_dev2.json', encoding='utf-8') as F:
#    credentials = json.loads(F.read())


db = Database(credentials = credentials)

entity_name = 'Clients03'
#(params, metadata)= metadata.retrieve_entity_type_metadata(_db=db, logical_name = "Clients2")
#params = entity.get_server_params()
print ( " new  entity params" )
#print (params)
#print ( " new  entity metadata" )
#print (metadata)
DimsPreload(   output_item = 'http_preload_done'),

#entity.exec_local_pipeline()

'''
new = EntityDataGenerator(
    ids=self.ids,
    output_item=self.output_item
)

print new
'''
#dims_fun = DimsPreload(
#                output_item = 'dims_preload_done'),
#if ids is None:
#gen = EntityDataGenerator(ids=ids, output_item='entity_data_generator')
#self.data_item_domain[key] = values
'''
view entity data
'''
#Get get_entity_type
#print ( "Read Table of new  entity" )
#df = db.read_table(table_name=entity_name, schema=db_schema)
#print(df.head())

print ( "Done" )
