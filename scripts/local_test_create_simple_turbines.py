import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from ai.functions import TurbineHTTPPreload
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
from ai import settings
from scripts.simple_mfg_entities import Equipment
import datetime as dt
from iotfunctions import pipeline as pp
from iotfunctions.pipeline import JobController
from iotfunctions.enginelog import EngineLogging
EngineLogging.configure_console_logging(logging.DEBUG)
logger = logging.getLogger(__name__)

#with open('credentials.json', encoding='utf-8') as F:
#db_schema = 'bluadmin' #  set if you are not using the default
#with open('./Monitor-Demo-Credentials.json', encoding='utf-8') as F:
#    credentials = json.loads(F.read())
print("here")
db_schema = 'bluadmin' #  set if you are not using the default
with open('../bouygues-beta-credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
#db_schema = 'dash100462'  # replace if you are not using the default schema
#with open('credentials_dev2.json', encoding='utf-8') as F:
#    credentials = json.loads(F.read())
#db_schema = 'bluadmin' #  set if you are not using the default
#with open('credentials_MAS-Demo.json', encoding='utf-8') as F:
#    credentials = json.loads(F.read())

print("here db")
db = Database(credentials = credentials)

entity_type_name = 'ACME_Compressors'
entityType = entity_type_name

db.drop_table(entity_type_name, schema = db_schema)
print("here entity")
entity = Equipment(name = entity_type_name,
                db = db,
                db_schema = db_schema,
                description = "Smart Connect Operations Control Center",
                )

entity.register(raise_error=False)
# You must unregister_functions if you change the mehod signature or required inputs.
#db.unregister_functions(["DataHTTPPreload"])
#db.register_functions([TurbineHTTPPreload])
print("registered function")

#entity.add_slowly_changing_dimension(self,property_name,datatype,**kwargs):
print("here make_dimension")
entity.make_dimension()

meta = db.get_entity_type(entityType)
jobsettings = {'_production_mode': False,
               '_start_ts_override': dt.datetime.utcnow() - dt.timedelta(days=10),
               '_end_ts_override': (dt.datetime.utcnow() - dt.timedelta(days=1)),  # .strftime('%Y-%m-%d %H:%M:%S'),
               '_db_schema': 'BLUADMIN',
               'save_trace_to_file': True}

logger.info('Instantiated create compressor job')

job = JobController(meta, **jobsettings)
#job.execute()

entity.exec_local_pipeline()

'''
view entity data
'''
print ( "Read Table of new  entity" )
df = db.read_table(table_name=entity_type_name, schema=db_schema)
print(df.head())

print ( "Done registering  entity" )
