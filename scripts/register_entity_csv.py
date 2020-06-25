import json
import logging
import sqlalchemy
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
# from custom.functions import InvokeModel
from iotfunctions.metadata import EntityType
from iotfunctions.metadata import BaseCustomEntityType
from iotfunctions.db import Database
from iotfunctions.base import BaseTransformer
from iotfunctions.bif import EntityDataGenerator
#from iotfunctions.enginelog import EngineLogging
from iotfunctions import system_function
from custom import settings
import datetime as dt

import sys
import pandas as pd
import numpy as np
import csv
#EngineLogging.configure_console_logging(logging.DEBUG)

'''
# Replace with a credentials dictionary or provide a credentials
# Explore > Usage > Watson IOT Platform Analytics > Copy to clipboard
# Past contents in a json file.
'''
credentials_path = 'credentials/monitor-credentials.json'
with open(credentials_path, encoding='utf-8') as F:
    credentials = json.loads(F.read())

'''
Developing Test Pipelines
-------------------------
When creating a set of functions you can test how they these functions will
work together by creating a test pipeline.
'''


# t = BaseTransformer()
'''
Create a database object to access Watson IOT Platform Analytics DB.
'''
db = Database(credentials = credentials)
db_schema = None #  set if you are not using the default

entity_name = "shell_turbine_demo2" #settings.ENTITY_NAME

if (len(sys.argv) > 1):
    csv_path = sys.argv[1]
else:
    csv_path = "./GasTurbineEntityDefinition.csv"
#     print("Please provide path to csv")
#     exit()

metrics = []
functions = []
dimensions = []

# df = db.read_table(table_name=entity_name, schema=db_schema)

with open(csv_path, mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            print("Column names are %s" % {", ".join(row)})
            line_count += 1
        else:
            name = row["Point"]
            print("Name %s" % name)
            type = row["DataType"]
            print("Type %s" % type)
            # '''
            # Create metric
            if row["Point_Data_Type"] == "S":
                print("________________________ Point point_data_type  %s " % row["Point_Data_Type"])
                print("________________________ Point db function name  %s " % row["Function"])
                name = row['Point']
                type = row['DataType']
                # metric = {
                #     'name':name,
                #     'type':type
                # }
                if 'string' in type.lower(): # string requires length
                    metrics.append(Column(name, getattr(sqlalchemy, type)(50)))
                else:
                    metrics.append(Column(name, getattr(sqlalchemy, type)()))

            # '''

            # Create Dimension
            if row["Point_Data_Type"] == "C":
                print("________________________ Point value  %s " % row["Value"])
                print("________________________ Point point_data_type  %s " % row["Point_Data_Type"].replace(' ', '_'))
                print("________________________ Point db function name  %s " % row["Function"])
                name = row['Point']
                type = row['DataType']
                value = {
                  "entity_id": row['Dim_Entity ID Value'],
                  "value": row['Value']
                }
                dimension = {
                    'name': name,
                    'type': type,
                    'value': value
                }
                dimensions.append(dimension)

            # Create Function
            if row["Point_Data_Type"] == "F":
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
                # if row["Function"] == "PythonExpression":
                #     expression = "df[%s].max()" % input_metrics
                #     f = bif.PythonExpression(expression=expression, output_name=output_name)
                # else :
                #     system_function.AggregateItems(row['Input Argument list of metric names'].split(',') , row["Function"])

                # TODO, use aggre
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

print("metrics")
print(metrics)
print("functions")
print(functions)
print("dimensions")
print(dimensions)

columns = tuple(metrics)
entity = BaseCustomEntityType(entity_name, db,
            columns = columns,
            functions = functions
)

# for d in dimensions:
#     entity.make_dimension(self.dim_table_name,
#                Column('dimension_1', String(50)), # add dimension_1
#                **{'schema': schema})


print(entity)
entity.register()
entity.generate_data(days=0.5)
entity.publish_kpis()
exit()
