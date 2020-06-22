import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from ai.functions import TurbineHTTPPreload
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging
from ai import settings
from scripts.manufacturing_entities import Equipment
from iotfunctions import pipeline as pp
from iotfunctions.pipeline import JobController
from iotfunctions.enginelog import EngineLogging
import datetime as dt

EngineLogging.configure_console_logging(logging.DEBUG)
logging = logging.getLogger(__name__)

'''
# Replace with a credentials dictionary or provide a credentials
# Explore > Usage > Watson IOT Platform Analytics > Copy to clipboard
# Past contents in a json file.
# Use pip install git+https://@github.com/ibm-watson-iot/functions.git@beta
# Works upto createing 10 entities,  dimensions,  metrics and works with turbine-ssimulator
'''

db_schema = 'bluadmin' #  set if you are not using the default
with open('credentials_MAS-Demo.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

#db_schema = 'bluadmin' #  set if you are not using the default
#with open('bouygues-beta-credentials.json', encoding='utf-8') as F:
#    credentials = json.loads(F.read())

#db_schema = 'bluadmin'  # set if you are not using the default
#with open('credentials_Monitor-Demo.json', encoding='utf-8') as F:
#    credentials = json.loads(F.read())

#db_schema = 'dash100462'  # replace if you are not using the default schema
#with open('credentials_dev2.json', encoding='utf-8') as F:
#    credentials = json.loads(F.read())

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

'''
To do anything with IoT Platform Analytics, you will need one or more entity type.
You can create entity types through the IoT Platform or using the python API as shown below.
The database schema is only needed if you are not using the default schema. You can also rename the timestamp.
'''
entity_type_name = 'ACME_Compressors'
entityType = entity_type_name
#db.drop_table(entity_name, schema = db_schema)

entity = Equipment(name = entity_type_name,
                db = db,
                db_schema = db_schema,
                description = "Manufacturing Operations Command Center",
                generate_days = 1,
                drop_existing = False)

#entity.register(raise_error=False)
# You must unregister_functions if you change the mehod signature or required inputs.
#db.unregister_functions(["DataHTTPPreload"])
#db.unregister_functions(["TurbineHTTPPreload"])
db.register_functions([TurbineHTTPPreload])

meta = db.get_entity_type(entityType)
jobsettings = {}
jobsettings = {'_production_mode': False,
               '_start_ts_override': dt.datetime.utcnow() - dt.timedelta(days=10),
               '_end_ts_override': (dt.datetime.utcnow() - dt.timedelta(days=1)),  # .strftime('%Y-%m-%d %H:%M:%S'),
               '_db_schema': 'BLUADMIN',
               'save_trace_to_file': True}

logger.info('Instantiated create compressor job')

job = JobController(meta, **jobsettings)
job.execute()

entity.exec_local_pipeline()

'''
view entity data
'''
print ( "Read Table of new  entity" )
df = db.read_table(table_name=entity_name, schema=db_schema)
print(df.head())

print ( "Done registering  entity" )
