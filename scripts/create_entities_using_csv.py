import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.metadata import EntityType
from scripts.simple_mfg_entities import Turbines
from iotfunctions.db import Database
import datetime as dt
from iotfunctions.base import BaseTransformer

# from iotfunctions.bif import EntityDataGenerator
# from ai import settings
# from iotfunctions.pipeline import JobController
# from iotfunctions.enginelog import EngineLogging
# EngineLogging.configure_console_logging(logging.DEBUG)
logger = logging.getLogger(__name__)

import sys

# import pandas as pd
# import numpy as np

# import csv
# import sqlalchemy


logging.debug("start")

if (len(sys.argv) > 0):
    entity_type_name = sys.argv[1]
    entityType = entity_type_name
    asset_tags_file = sys.argv[2]
    asset_series_data_file = sys.argv[3]
    logging.debug("entity_name %s" % entity_type_name)
    logging.debug("asset_tags %s" % asset_tags_file)
    logging.debug("asset_scan_data %s" % asset_series_data_file)
else:
    logging.debug("Please provide path to csv file as script argument")
    exit()

'''
# Replace with a credentials dictionary or provide a credentials
# Explore > Usage > Watson IOT Platform Analytics > Copy to clipboard
# Past contents in a json file.
'''
logging.debug("Read credentials")
with open('../bouygues-beta-credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Create a database object to access Watson IOT Platform Analytics DB.
'''
db = Database(credentials=credentials)
db_schema = 'bluadmin'  # set if you are not using the default

print("Delete existing Entity Type")
db.drop_table(entity_type_name, schema=db_schema)

# print("Unregister EntityType")
####
# Required input args for creating an entity type
# self, name, db, columns=None, constants=None, granularities=None, functions=None,
#                 dimension_columns=None, generate_days=0, generate_entities=None, drop_existing=False, db_schema=None,
#                 description=None, output_items_extended_metadata=None, **kwargs)
# https://github.com/ibm-watson-iot/functions/blob/60002500117c4559ed256cb68204c71d2e62893d/iotfunctions/metadata.py#L2237
###
logging.debug("Create Entity Type")
entity = Turbines(asset_tags_file=asset_tags_file,  name=entity_type_name, db=db, db_schema=db_schema, description="Equipment Turbines",
                  generate_entities=True, table_name=entity_type_name)

logging.debug("Register EntityType")
entity.register(raise_error=False)

#logging.debug("Register Constants")
#entity.db.register_constants(entity.ui_constants)

logging.debug("Create Dimension")
entity.make_dimension(None, entity.dimension_columns)

logging.debug("Read Metrics Data")
entity.read_meter_data(asset_series_data_file=asset_series_data_file)

# logging.debug("Create Calculated Metrics")
# entity.publish_kpis()

meta = db.get_entity_type(entity_type_name)

'''
#Used to execute pipeline in Monitor
jobsettings = {'_production_mode': False,
               '_start_ts_override': dt.datetime.utcnow() - dt.timedelta(days=10),
               '_end_ts_override': (dt.datetime.utcnow() - dt.timedelta(days=1)),  # .strftime('%Y-%m-%d %H:%M:%S'),
               '_db_schema': db_schema,
               'save_trace_to_file': True}
logging.info('Instantiated create  job')
job = JobController(meta, **jobsettings)
job.execute()
entity.exec_local_pipeline()
'''

# Check to make sure table was created
print("DB Name %s " % entity_type_name)
print("DB Schema %s " % db_schema)
df = db.read_table(table_name=entity_type_name, schema=db_schema)
print(df.head())  # entity.read_meter_data( input_file="None")
