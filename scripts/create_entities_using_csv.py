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
from iotfunctions import system_function
EngineLogging.configure_console_logging(logging.DEBUG)
logger = logging.getLogger(__name__)

# from iotfunctions.db import http_request

import sys
import pandas as pd
import numpy as np
import requests
import csv
import sqlalchemy


logging.debug("start")

if (len(sys.argv) > 0):
    entity_type_name = sys.argv[1]
    # entityType = "GasTurbinesKB3"
    input_file = sys.argv[2]
    logging.debug("entity_name %s" %entity_type_name)
    logging.debug("input_file %s" % input_file)
else:
    logging.debug("Please provide path to csv file as script argument")
    exit()


methods = {'count': 'Count', 'std': 'Std', 'product': 'Product', 'last': 'Last', 'min': 'Minimum', 'max': 'Maximum', 'sum': 'Sum', 'median': 'Median', 'var': 'Var', 'first': 'First', 'count_distinct': 'Count_distinct', 'mean': 'Mean'}
functions = []
rest_functions = []

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
                    # input_metrics = row['Input Arg Value'].lower().replace(' ', '').split('|') # merge with "Input Argument list of metric names"
                    # function_name =
                    input_metrics = row['Input Arg Value'].lower().replace(' ', '').split('|') # merge with "Input Argument list of metric names"
                    if len(input_metrics[0]) < 1:
                        print("function requires input metrics, skipping")
                        continue
                    source = input_metrics[0]
                    function_name = row['Function']
                    if function_name in methods.keys():
                        function_name = methods[function_name]
                        input = {"source": source}
                        output_name = function_name.lower() + entity_type_name
                        # functions.append(f)
                    elif function_name == 'ratio':
                        # expression = "df['%s'].iloc[-1] / df['%s'].iloc[-1]" % (input_metrics[0], input_metrics[1])
                        function_name = "PythonExpression"
                        expression = "df['%s'] / df['%s']" % (input_metrics[0], input_metrics[1])
                        input = {"expression": expression}
                        output_name = entity_type_name.lower() + "ratio"
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                        functions.append(f)
                        continue
                    elif function_name == 'multiply':
                        # expression = "df['%s'].iloc[-1] / df['%s'].iloc[-1]" % (input_metrics[0], input_metrics[1])
                        function_name = "PythonExpression"
                        expression = "df['%s'] / df['%s']" % (input_metrics[0], input_metrics[1])
                        input = {"expression": expression}
                        output_name = entity_type_name.lower() + "multiply"
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                        functions.append(f)
                    else:
                        function_name = "PythonExpression"
                        expression = input_metrics[0]
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                        functions.append(f)
                        continue
                    # else:
                        # function_name = methods[function_name]
                    # grain = ["Daily", "Hourly", ]
                    payload = {
                        "functionName": function_name,
                        "granularity": "Daily",
                        "input": input,
                        "output": {
                            "name": output_name
                        },
                        "schedule": {},
                        "backtrack": {},
                        "enabled": True
                    }

                    print(payload)
                    rest_functions.append(payload)
                    continue
                    '''
                    # dynamically get method from bif
                    method = getattr(bif, row['Function'])
                    if method:
                        f =
                    '''
                    # TODO, use aggregator
                    #
                    #
                    # system_function.AggregateItems(row['Input Argument list of metric names'].split(',') , row["Function"])
                    '''
                    agg_methods = ['sum', 'count', 'count_distinct', 'min', 'max', 'mean', 'median', 'std', 'var', 'first', 'last', 'product']
                    if row['Function'] in agg_methods:
                        print("adding " + row['Function'] + " aggregator method")
                        f = system_function.AggregateItems([input_metrics[0]] , row['Function'])
                        # f.PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'
                    '''
                    if row["Function"] == 'max':
                        # expression = "df['%s'].max()" % input_metrics[0]
                        # f = bif.PythonExpression(expression=expression, output_name=output_name)
                        print("adding " + row['Function'] + " aggregator method")
                        f = system_function.AggregateItems([input_metrics[0]] , 'max')
                        # f.PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'
                        # f = bif.Maximum
                    elif row["Function"] == 'min':
                        expression = "df['%s'].min()" % input_metrics[0]
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                    elif row["Function"] == 'sum':
                        expression = "df['%s'].sum()" % input_metrics[0]
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                    elif row["Function"] == 'multiply':
                        print(input_metrics)
                        # expression = "df['%s'].iloc[-1] * df['%s'].iloc[-1]" % (input_metrics[0], input_metrics[1])
                        expression = "df['%s'] * df['%s']" % (input_metrics[0], input_metrics[1])
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                    elif row["Function"] == 'ratio':
                        # expression = "df['%s'].iloc[-1] / df['%s'].iloc[-1]" % (input_metrics[0], input_metrics[1])
                        expression = "df['%s'] / df['%s']" % (input_metrics[0], input_metrics[1])
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                    elif row["Function"] == 'last':
                        # expression = "df['%s'].iloc[-1]" % input_metrics[0]
                        expression = "df['%s']" % input_metrics[0]
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                    elif row["Function"] == "PythonExpression":
                        expression = row['Input Arg Value']
                        f = bif.PythonExpression(expression=expression, output_name=output_name)
                    # functions.append(f)
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
# with open(credentials_path, encoding='utf-8') as F:
with open(credentials_path) as F:
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
print("printing functions")
print(functions)

print("printing rest_functions")
print(rest_functions)
# exit()
# entity_type_name = "GasTurbinesKB2"

logging.debug("Creating Entity Type")
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
entity.register() #raise_error=True, publish_kpis=True)

logging.debug("Generating data")
entity.generate_data(days=0.5)

logging.debug("Publishing functions")
logging.debug(functions)
entity.publish_kpis()

for payload in rest_functions:
    # entity_type.db.http_request(object_type='function', object_name=name, request='DELETE', payload=payload)
    print("posting payload")
    print(payload)
    url = "https://%s/api/kpi/v1/%s/entityType/%s/kpiFunction" % (credentials['iotp']['asHost'] ,credentials['tenantId'], entity_type_name)
    headers = {'Content-Type': "application/json", 'X-Api-Key': credentials['iotp']['apiKey'],
           'X-Api-Token': credentials['iotp']['apiToken'], 'Cache-Control': "no-cache", }
    r = requests.post(url, headers=headers, json=payload)
    if r.status_code == 200:
        print("function created")
    else:
        print("failure creating function")
        print(r.status_code)
        print(r.text)
    # entity.db.http_request('kpiFunctions', entity_type_name, 'POST', payload)
    # exit()

exit()

# logging.debug("Create Dimension")
# entity.make_dimension()

# logging.debug("Read Metrics Data")
# entity.read_meter_data()

#logging.debug("Create Calculated Metrics")
entity.publish_kpis()

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
