import json
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.metadata import EntityType
from scripts.simple_mfg_entities import Turbines
from iotfunctions.db import Database
import datetime as dt
from iotfunctions.base import BaseTransformer
#from iotfunctions.bif import EntityDataGenerator
#from ai import settings
from iotfunctions.pipeline import JobController
from iotfunctions.enginelog import EngineLogging
EngineLogging.configure_console_logging(logging.DEBUG)
logger = logging.getLogger(__name__)


import sys
import pandas as pd
import numpy as np

import csv
import sqlalchemy


logging.debug("start")

if (len(sys.argv) > 0):
    entity_type_name = sys.argv[1]
    entityType = "GasTurbines3"
    input_file = sys.argv[2]
    logging.debug("entity_name %s" %entity_type_name)
    logging.debug("input_file %s" % input_file)
else:
    logging.debug("Please provide path to csv file as script argument")
    exit()


print("Read CSV File")
with open(input_file, mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    line_count = 0
    point_dimension_values = {
        "label": "",
        "units": "",
        "parameter_name": ""
    }
    metrics = []
    dims = []
    constants = []
    functions =[]
    # dimension_columns = []

    for row in csv_reader:
        if line_count == 0:
            logging.debug("Column names are %s" % {", ".join(row)})
            line_count += 1
        else:
            try:
                parameter_name = row["Point"].replace(' ', '_')
                logging.debug("Name %s" % parameter_name)
                type = row["DataType"]
                logging.debug("Type %s" % type)
                parameter_value = row["Value"].replace(' ', '_')
                logging.debug("Value %s" % parameter_value)

                if parameter_name == "":
                    break # No more rows

                # Create metric
                if row["Point_Data_Type"] == "S":
                    print("________________________ Point point_data_type  %s " % row["Point_Data_Type"])
                    print("________________________ Point db function name  %s " % row["Function"])
                    name = row['Point']
                    type = row['DataType']
                    if 'string' in type.lower(): # string requires length
                        metrics.append(Column(name, getattr(sqlalchemy, type)(50)))
                    else:
                        metrics.append(Column(name, getattr(sqlalchemy, type)()))

                '''
                # Create dimension
                if row["Point_Data_Type"] == "D":
                    logging.debug("________________________ Point point_data_type dimension")
                    dim_to_add = {'parameter_name': parameter_name, 'type': type, 'value':parameter_value}
                    dims.append(dim_to_add)
                    for dim in dims:
                        logging.debug("Adding dimension name to entity type %s" %dim['parameter_name'] )
                        logging.debug("Adding metric type to entity type %s" %dim['type'] )
                        unallowed_chars = "!@#$()"
                        for char in unallowed_chars:
                            dim['parameter_name'] = dim['parameter_name'].replace(char, "")
                            dimension_columns.append(Column(metric['parameter_name'], String(50)))
                        logging.debug("Adding cleansed dimension name to entity type %s" % dim['parameter_name'])


                # Create Constant
                if row["Point_Data_Type"] == "C":
                    logging.debug("________________________ Point point_data_type constant")
                    constant_to_add = {'parameter_name': parameter_name, 'type': type, 'value':parameter_value}
                    constants.append(constant_to_add)
                '''

                # Create Function
                if row["Point_Data_Type"] == "F":
                    # TODO, we need to standardize how many args can be passed in. And handle them properly
                    # bif.PythonExpression
                    # bif.PythonExpression(expression='df["temp"]*df["pressure"]', output_name='volume')
                    print("________________________ Point point_data_type  %s " % row["Point_Data_Type"])
                    print("________________________ Point db data_type  %s " % row["DataType"])
                    print("________________________ Point db function name  %s " % row["Function"])

                    output_name = row['Point']
                    expression = row['Function']
                    print("row['Input Arg Value']")
                    print(row['Input Arg Value'])
                    input_metrics = row['Input Arg Value'].lower().replace(' ', '').split('|') # merge with "Input Argument list of metric names"
                    print(input_metrics)
                    '''
                    # dynamically get method from bif
                    method = getattr(bif, row['Function'])
                    if method:
                        f =
                    '''
                    # TODO, use aggregator
                    # from iotfunctions import system_function
                    # system_function.AggregateItems(['Suction'] , 'max')
                    # system_function.AggregateItems(row['Input Argument list of metric names'].split(',') , row["Function"])
                    if row["Function"] == 'max':
                        expression = "df['%s'].max()" % input_metrics[0]
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                        # f = bif.Maximum
                        functions.append(f)
                    elif row["Function"] == 'min':
                        expression = "df['%s'].min()" % input_metrics[0]
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                        functions.append(f)
                    elif row["Function"] == 'sum':
                        expression = "df['%s'].sum()" % input_metrics[0]
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                        functions.append(f)
                    elif row["Function"] == 'multiply':
                        print(input_metrics)
                        # expression = "df['%s'].iloc[-1] * df['%s'].iloc[-1]" % (input_metrics[0], input_metrics[1])
                        expression = "df['%s'] * df['%s']" % (input_metrics[0], input_metrics[1])
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                        functions.append(f)
                    elif row["Function"] == 'ratio':
                        # expression = "df['%s'].iloc[-1] / df['%s'].iloc[-1]" % (input_metrics[0], input_metrics[1])
                        expression = "df['%s'] / df['%s']" % (input_metrics[0], input_metrics[1])
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                        functions.append(f)
                    elif row["Function"] == 'last':
                        # expression = "df['%s'].iloc[-1]" % input_metrics[0]
                        expression = "df['%s']" % input_metrics[0]
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                        functions.append(f)
                    elif row["Function"] == "PythonExpression":
                        expression = row['Input Arg Value']
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                        functions.append(f)
            except:
                logging.debug(sys.exc_info()[0])  # the exception instance
                break




'''
# Replace with a credentials dictionary or provide a credentials
# Explore > Usage > Watson IOT Platform Analytics > Copy to clipboard
# Past contents in a json file.
'''
logging.debug("Read credentials")
credentials_path = "/Users/kkbankol@us.ibm.com/projects/maximo_anomaly/credentials/monitor-credentials.json"
with open(credentials_path, encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Create a database object to access Watson IOT Platform Analytics DB.
'''
db = Database(credentials = credentials)
db_schema = None
# db_schema = 'bluadmin' #  set if you are not using the default

'''
print("Delete existing Entity Type")
db.drop_table(entity_type_name, schema = db_schema)
'''

#print("Unregister EntityType")
####
# Required input args for creating an entity type
# self, name, db, columns=None, constants=None, granularities=None, functions=None,
#                 dimension_columns=None, generate_days=0, generate_entities=None, drop_existing=False, db_schema=None,
#                 description=None, output_items_extended_metadata=None, **kwargs)
# https://github.com/ibm-watson-iot/functions/blob/60002500117c4559ed256cb68204c71d2e62893d/iotfunctions/metadata.py#L2237
###
columns = tuple(metrics)
print(columns)
print(functions)

logging.debug("Create Entity Type")
entity = Turbines(name = entity_type_name,
                    db = db,
                    db_schema = db_schema,
                    columns = columns,
                    functions = functions,
                    description = "Equipment Turbines",
                    generate_entities=True,
                    table_name=entity_type_name
                )

logging.debug("Register EntityType")
entity.register(raise_error=False)


entity.generate_data(days=0.5)
entity.publish_kpis()
# logging.debug("Create Dimension")
# entity.make_dimension()

# logging.debug("Read Metrics Data")
# entity.read_meter_data()

#logging.debug("Create Calculated Metrics")
#entity.publish_kpis()

meta = db.get_entity_type(entity_type_name)
jobsettings = {'_production_mode': False,
               '_start_ts_override': dt.datetime.utcnow() - dt.timedelta(days=10),
               '_end_ts_override': (dt.datetime.utcnow() - dt.timedelta(days=1)),  # .strftime('%Y-%m-%d %H:%M:%S'),
               '_db_schema': db_schema,
               'save_trace_to_file': True}

logging.info('Instantiated create  job')

job = JobController(meta, **jobsettings)
job.execute()

entity.exec_local_pipeline()

# Check to make sure table was created
print("DB Name %s " % entity_type_name)
print("DB Schema %s " % db_schema)
df = db.read_table(table_name=entity_type_name, schema=db_schema)
print( df.head())

## TODO, add dimension calls

#entity.read_meter_data( input_file="None")
