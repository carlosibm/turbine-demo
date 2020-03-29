import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
import numpy as np
import pandas as pd
import json


#from iotfunctions.base import BaseTransformer
from iotfunctions.base import BasePreload
from iotfunctions import ui
from iotfunctions.db import Database
from iotfunctions import bif
#import datetime as dt
import datetime
import urllib3
import xml.etree.ElementTree as ET
from ai import settings
import requests

from iotfunctions.enginelog import EngineLogging
EngineLogging.configure_console_logging(logging.DEBUG)

# Specify the URL to your package here.
# This URL must be accessible via pip install
#PACKAGE_URL = 'git+https://github.com/madendorff/functions@'
PACKAGE_URL = 'git+https://github.com/fe01134/turbine-demo@'


class TurbineHTTPPreload(BasePreload):
    '''
    Do a HTTP request as a preload activity. Load results of the get into the Entity Type time series table.
    HTTP request is experimental
    '''

    out_table_name = None

    def __init__(self,  username, password, request, url, headers = None, body = None, column_map = None, output_item  = 'http_preload_done'):

        if body is None:
            body = {}

        if headers is None:
            headers = {}

        if column_map is None:
            column_map = {}

        super().__init__(dummy_items=[],output_item = output_item)

        # create an instance variable with the same name as each arg
        self.username = username
        logging.debug('self.username %s' %self.username)
        self.password = password
        logging.debug('self.password %s' %self.password)
        if url == None:
            self.tenant = settings.BI_TENANT_ID
        else:
            self.tenant = url
        logging.debug('tenantid self.tenant %s' %self.tenant)
        self.request = request
        logging.debug('self.request %s' %self.request)
        self.headers = headers
        logging.debug('headers %s' %headers)
        self.body = body
        logging.debug('body %s' %body)
        self.column_map = column_map
        logging.debug('column_map %s' %column_map)

        # do not do any processing in the init() method. Processing will be done in the execute() method.

    def getTurbines (self, data = None ):
        turbines = []
        for turbine in data:
            logging.debug("parseTurbines  turbine  %s " %turbine)
            turbines.append(turbine)
        return turbines

    def getTemperatures (self, data = None ):
        temperatures = []
        for temperature in data:
            logging.debug("parseTemperature  temperature  %s " %temperature)
            temperatures.append(temperature)
        return temperatures

    def getPressures (self, data = None ):
        pressures = []
        for pressure in data:
            logging.debug("parsePressures  pressure  %s " %pressure)
            pressures.append(pressure)
        return pressures

    def getAssets (self, ):
        # Gets turbine simulation data from https://turbine-simulator.mybluemix.net/v1/api/#!/default/get_reading
        # energy_metrics req.data  b'{"value":16.3,"unit":"MWh","compare_percent":7.34,"trend":"DOWN","trend_status":"GREEN"}'

        # Initialize
        net_metrics_data = {}
        #metrics_TURBINE_ID = []
        #metrics_TEMPERATURE  = []
        #metrics_PRESSURE  = []

        #response_back = { "deviceid" : ["A101","B102"],
        #                "TURBINE_ID" : ["A101","B102"],
        #                "TEMPERATURE" : [37,39],
        #                "PRESSURE" : [92,89]}
        logging.debug("Getting list of Assets from Turbine Simulation REST API")
        uri = self.tenant
        header = { 'Accept' : 'application/json' }
        '''
        response = requests.get(
                         url = uri,
                         headers = headers)
        '''
        response = self.db.http.request('GET',
                                 uri,
                                 headers= header)
        logging.debug('getAssets response.text  %s' %response.data)

        if response.status == 200 or response.status == 201:
            logging.debug( "response data response" )
            metrics_json = json.loads(response.data.decode('utf-8'))
            logging.debug( metrics_json )

            for metric in metrics_json.keys():
                logging.debug( "looping on metric key %s " %metric )
                logging.debug( "looping on metrics %s " %metrics_json[metric] )
                if metric == 'TEMPERATURE':
                    logging.debug( "Found TEMPERATURE %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'PREDICT_TEMPERATURE':
                    logging.debug( "Found PREDICT_TEMPERATURE %s " %metrics_json[metric])
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'PRESSURE':
                    logging.debug( "Found PRESSURE %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'PREDICT_PRESSURE':
                    logging.debug( "Found PREDICT_PRESSURE %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'TURBINE_ID':
                    logging.debug( "Found TURBINE_ID %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'STEP':
                    logging.debug( "Found STEP %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'TEMP_X':
                    logging.debug( "Found TEMP_X %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'TEMP_Y':
                    logging.debug( "Found TEMP_Y %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'PRESS_X':
                    logging.debug( "Found PRESS_X %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'PREDICT_PRESS_X':
                    logging.debug( "Found PREDICT_PRESS_X %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'PRESS_Y':
                    logging.debug( "Found PRESS_Y %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'A_TEMP_X':
                    logging.debug( "Found A_TEMP_X %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'A_TEMP_Y':
                    logging.debug( "Found A_TEMP_Y %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'A_PRESS_X':
                    logging.debug( "Found A_PRESS_X %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'A_PRESS_Y':
                    logging.debug( "Found A_PRESS_Y %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'B_TEMP_X':
                    logging.debug( "Found B_TEMP_X %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'B_TEMP_Y':
                    logging.debug( "Found B_TEMP_Y %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'B_PRESS_X':
                    logging.debug( "Found B_PRESS_X %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'B_PRESS_Y':
                    logging.debug( "Found B_PRESS_Y %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'CLIENT':
                    logging.debug( "Found CLIENT %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'ORG':
                    logging.debug( "Found ORG %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]
                if metric == 'FUNCTION':
                    logging.debug( "Found FUNCTION %s " %metrics_json[metric] )
                    net_metrics_data[metric] = metrics_json[metric]

            logging.debug( "net_metrics_data %s " %net_metrics_data )
            rows = len(net_metrics_data)

            '''
            turbines = self.getTurbines(metrics_json['TURBINE_ID'])
            logging.debug( "turbines %s " %turbines )
            rows = len(turbines)
            logging.debug( "length of turbines %s " %rows )

            #logging.debug( "metrics_json TEMPERATURE %s " %metrics_json['TEMPERATURE'] )
            temperatures = self.getTemperatures(metrics_json['TEMPERATURE'] )
            logging.debug( "temperatures %s " %temperatures )

            pressures = self.getPressures( metrics_json['PRESSURE'])
            logging.debug( "pressures %s " %pressures      )
            '''
        else:
            # This means something went wrong.
            logging.debug("Error calling REST API. Using Hard coded values")
            net_metrics_data =  {'A_TEMP_Y': [10, 20], 'PRESSURE': [172.64187332977474, -191.7466699309894], 'TURBINE_ID': ['A101', 'B102'], 'PRESS_Y': [30, 60], 'TEMP_X': [10, 40], 'A_TEMP_X': [10, 40], 'TEMPERATURE': [69.0567493319099, -78.44181951722294], 'PRESS_X': [80, 80], 'PREDICT_PRESS_X': [80, 80], 'PREDICT_PRESSURE': [172.64187332977474, -191.7466699309894], 'STEP': [21.0, 21.0], 'B_PRESS_Y': [30, 60], 'A_PRESS_Y': [30, 50], 'A_PRESS_X': [30, 60], 'TEMP_Y': [10, 20], 'B_TEMP_X': [10, 40], 'B_PRESS_X': [30, 50], 'B_TEMP_Y': [10, 20], 'PREDICT_TEMPERATURE': [69.0567493319099, -78.44181951722294]}
            rows = len(net_metrics_data)
            #metrics_TURBINE_ID.append("NA")
            #metrics_TEMPERATURE.append(0.0)
            #metrics_PRESSURE.append(0.0)
            rows = 0
        #return turbines, temperatures, pressures, rows
        logging.debug( "return net_metrics_data %s " %net_metrics_data      )
        logging.debug("length rows %d" %rows )
        return net_metrics_data, rows

    def execute(self, df, start_ts = None,end_ts=None,entities=None):

        entity_type = self.get_entity_type()
        self.db = entity_type.db
        #self.encoded_body = json.dumps(self.body).encode('utf-8')
        #self.encoded_headers = json.dumps(self.headers).encode('utf-8')

        # This class is setup to write to the entity time series table
        # To route data to a different table in a custom function,
        # you can assign the table name to the out_table_name class variable
        # or create a new instance variable with the same name

        if self.out_table_name is None:
            table = entity_type.name
        else:
            table = self.out_table_name
        schema = entity_type._db_schema

        # Call external service to get device data.
        metrics_json, rows = self.getAssets()
        #metrics_TURBINE_ID, metrics_TEMPERATURE, metrics_PRESSURE, rows = self.getAssets()

        # Create Numpy array using Building Insights energy usage data
        response_data = {}
        (metrics,dates,categoricals,others) = self.db.get_column_lists_by_type(
            table = table,
            schema= schema,
            exclude_cols = []
        )

        for o in others:
            logging.debug('metrics others %s ' %o)
            #response_data[0] = np.random.normal(0,1,rows)
            #response_data[0] = np.random.normal(0,1,rows)
            #logging.debug('metrics data %s ' %response_data[m])

        for m in metrics:
            logging.debug('metrics  -- using str  %s ' %m )
            logging.debug('type is %s ' %type(m) )
            #logging.debug('metrics  json value -- %s ' %metrics_json[ m ] )

            #  There is a bug in Analytics service that required caps for attributes
            # convert sqlalchemy.sql.elements.quoted_name to a string
            metrics_uppercase_str =  m.casefold().upper()
            logging.debug('metrics data m %s ' %metrics_uppercase_str )
            response_data[ m ] = np.array( metrics_json[ metrics_uppercase_str ] )
            #logging.debug('metrics data %s ' %response_data[m.casefold().upper()) ])
            '''
            response_data[ m ] = np.array( metrics_json[ m ] )
            '''

        for d in dates:
            logging.debug('dates %s ' %d)
            response_data[d] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
            logging.debug('dates data %s ' %response_data[d])

        '''
        Set dimensional data
        hardcode for now
        '''
        response_data[ 'CLIENT' ] =  ['Mariners Way MFG', 'Mariners Way MFG']
        response_data[ 'ORGANIZATION' ] =  ['Production', 'Production']
        response_data[ 'FUNCTION' ] =  ['Line 1', 'Line 2']

        '''
        # Create Numpy array using remaining entity metrics
        '''
        #logging.debug("length metrics_TEMPERATURE %d" %len(metrics_TEMPERATURE) )
        #logging.debug("length metrics_PRESSURE %d" %len(metrics_PRESSURE) )
        response_data['turbine_id'] = np.array( metrics_json['TURBINE_ID'] )
        #response_data['TEMPERATURE'] = np.array( metrics_TEMPERATURE )
        #response_data['PRESSURE'] = np.array( metrics_PRESSURE )
        #response_data['devicetype'] = np.array(metrics_TURBINE_ID)
        response_data['deviceid'] = np.array( metrics_json['TURBINE_ID'] )
        #response_data['eventtype'] = np.array(metrics_TURBINE_ID)
        #response_data['turbine_id'] = np.array( metrics_json['TURBINE_ID'] )
        #response_data['format'] = np.array(metrics_TURBINE_ID)
        #response_data['logicalinterface_id'] = np.array(metrics_TURBINE_ID)

        '''
        # Create a timeseries dataframe with data received Building Insights
        '''
        logging.debug('response_data used to create dataframe ===' )
        logging.debug( response_data)
        df = pd.DataFrame(data=response_data)
        logging.debug('Generated DF from response_data ===' )
        logging.debug( df.head() )
        df = df.rename(self.column_map, axis='columns')
        logging.debug('ReMapped DF ===' )
        logging.debug( df.head() )

        # Fill in missing columns with nulls
        required_cols = self.db.get_column_names(table = table, schema=schema)
        logging.debug('required_cols %s' %required_cols )
        missing_cols = list(set(required_cols) - set(df.columns))
        logging.debug('missing_cols %s' %missing_cols )
        if len(missing_cols) > 0:
            kwargs = {
                'missing_cols' : missing_cols
            }
            entity_type.trace_append(created_by = self,
                                     msg = 'http data was missing columns. Adding values.',
                                     log_method=logger.debug,
                                     **kwargs)
            for m in missing_cols:
                if m==entity_type._timestamp:
                    df[m] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
                elif m=='devicetype':
                    df[m] = entity_type.logical_name
                else:
                    df[m] = None

        # Remove columns that are not required
        df = df[required_cols]
        logging.debug('DF stripped to only required columns ===' )
        logging.debug( df.head() )

        # Write the dataframe to the IBM IOT Platform database table
        self.write_frame(df=df,table_name=table)
        kwargs ={
            'table_name' : table,
            'schema' : schema,
            'row_count' : len(df.index)
        }
        logging.debug('DF write_frame to table ===' )
        entity_type.trace_append(created_by=self,
                                 msg='Wrote data to table',
                                 log_method=logger.debug,
                                 **kwargs)
        return True

    @classmethod
    def build_ui(cls):
        '''
        Registration metadata
        '''
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UISingle(name='username',
                              datatype=str,
                              description='Username for Building Insignts Instance',
                              tags=['TEXT'],
                              required=True
                              ))
        inputs.append(ui.UISingle(name='password',
                              datatype=str,
                              description='Password for Building Insignts Instance',
                              tags=['TEXT'],
                              required=True
                              ))
        inputs.append(ui.UISingle(name='request',
                              datatype=str,
                              description='comma separated list of entity ids',
                              values=['GET','POST','PUT','DELETE']

                              ))
        inputs.append(ui.UISingle(name='url',
                                  datatype=str,
                                  description='request url',
                                  tags=['TEXT'],
                                  required=True
                                  ))
        inputs.append(ui.UISingle(name='headers',
                               datatype=dict,
                               description='request url',
                               required = False
                               ))
        inputs.append(ui.UISingle(name='body',
                               datatype=dict,
                               description='request body',
                               required=False
                               ))
        # define arguments that behave as function outputs
        outputs=[]
        outputs.append(ui.UIStatusFlag(name='output_item'))
        return (inputs, outputs)
