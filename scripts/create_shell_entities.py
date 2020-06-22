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
from iotfunctions.enginelog import EngineLogging
EngineLogging.configure_console_logging(logging.DEBUG)
logging = logging.getLogger(__name__)

#db_schema = 'bluadmin' #  set if you are not using the default
#with open('credentials_MAS-Demo.json', encoding='utf-8') as F:
#    credentials = json.loads(F.read())
print("here")
#with open('credentials.json', encoding='utf-8') as F:
db_schema = 'bluadmin' #  set if you are not using the default
with open('../bouygues-beta-credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
#db_schema = 'dash100462'  # replace if you are not using the default schema
#with open('credentials_dev2.json', encoding='utf-8') as F:
#    credentials = json.loads(F.read())
print("here db")
db = Database(credentials = credentials)


entity_name = 'ACME_Compressors'
entityType = entity_name
db_schema = None  # replace if you are not using the default schema
db.drop_table(entity_name, schema=db_schema)
entity = EntityType(entity_name, db,
                    bif.EntityDataGenerator(ids=['73000', '73001', '73002', '73003', '73004'], data_item='is_generated'),
                    **{'_timestamp': 'evt_timestamp', '_db_schema': db_schema})

# dimension columns
dimension_columns = []
dimension_columns.append(Column('business', String(50)))
dimension_columns.append(Column('site', String(50)))
dimension_columns.append(Column('equipment_type', String(50)))
dimension_columns.append(Column('train', String(50)))
dimension_columns.append(Column('service', String(50)))
dimension_columns.append(Column('asset_id', String(50)))

entity.register(raise_error=False)
db.register_functions([TurbineHTTPPreload])

df = db.read_table(table_name=entity_name, schema=db_schema)
print(df.head())

meta = db.get_entity_type(entityType)
jobsettings = {'_production_mode': False,
               '_start_ts_override': dt.datetime.utcnow() - dt.timedelta(days=10),
               '_end_ts_override': (dt.datetime.utcnow() - dt.timedelta(days=1)),  # .strftime('%Y-%m-%d %H:%M:%S'),
               '_db_schema': 'BLUADMIN',
               'save_trace_to_file': True}

logging.info('Instantiated create compressor job')

job = JobController(meta, **jobsettings)
job.execute()

entity.exec_local_pipeline()
