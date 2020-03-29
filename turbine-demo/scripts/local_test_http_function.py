import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from ai.functions import TurbineHTTPPreload
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
#with open('credentials_Monitor-Demo.json', encoding='utf-8') as F:
#with open('credentials.json', encoding='utf-8') as F:
with open('credentials_dev2.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Developing Test Pipelines
-------------------------
When creating a set of functions you can test how they these functions will
work together by creating a test pipeline.
'''


'''
Create a database object to access Watson IOT Platform Analytics DB.
'''
db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default

'''
To do anything with IoT Platform Analytics, you will need one or more entity type.
You can create entity types through the IoT Platform or using the python API as shown below.
The database schema is only needed if you are not using the default schema. You can also rename the timestamp.
'''
entity_name = 'Turbines'
# dash100462  Used in dev2
db_schema = 'dash100462'
# db_schema = None  # replace if you are not using the default schema
#db.drop_table(entity_name, schema = db_schema)

entity = EntityType(entity_name,db,
                    Column('Turbine_ID',String(50)),
                    Column('Temperature',Float()),
                    Column('Pressure',Float()),
                    Column('Volume', Float()),
                    TurbineHTTPPreload(request='GET',
                                    url='internal_test',
                                    output_item = 'http_preload_done'),
                    bif.PythonExpression(expression='df["Temperature"]*df["Pressure"]',
                                         output_name = 'Volume'),
                    **{
                      '_timestamp' : 'evt_timestamp',
                      '_db_schema' : db_schema
                      })
'''
When creating an EntityType object you will need to specify the name of the entity, the database
object that will contain entity data
After creating an EntityType you will need to register it so that it visible in the UI.
To also register the functions and constants associated with the entity type, specify
'publish_kpis' = True.
'''
entity.register(raise_error=False)
db.register_functions([TurbineHTTPPreload])

'''
To test the execution of kpi calculations defined for the entity type locally
use 'test_local_pipeline'.
A local test will not update the server job log or write kpi data to the AS data
lake. Instead kpi data is written to the local filesystem in csv form.
'''

entity.exec_local_pipeline()

'''
view entity data
'''

df = db.read_table(table_name=entity_name, schema=db_schema)
print(df.head())
