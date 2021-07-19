from __future__ import absolute_import
import os

import json
import argparse
import logging
from datetime import datetime
import pytz
import time

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.gcp.internal.clients import bigquery

class LoadtoBQ:

    def __init__(self):
        self.table_spec = bigquery.TableReference(
            projectId='gcptraining-319415',
            datasetId='weathercallsbq',
            tableId='gbq'
        )

        self.schema = 'city_name:string, \
                       temperature:float, \
                       minimum_temperature:float, \
                       maximum_temperature:float, \
                       humidity:float, \
                       visibility:integer, \
                       wind_speed:float, \
                       date_time:timestamp,\
                       conditions:string'
        self.log = {
            'city_name':' ',
            'temperature':' ',
            'minimum_temperature':' ',
            'maximum_temperature':' ',
            'humidity':' ',
            'visibility':' ',
            'wind_speed':' ',
            'date_time':' ',
            'conditions':' ',
        }


    def parse(self, line):
        
        data = line.split(',')
        self.log['city_name']=data[7]
        self.log['temperature']=data[0]
        self.log['minimum_temperature']=data[1]
        self.log['maximum_temperature']=data[2]
        self.log['humidity']= data[3]
        self.log['visibility']=data[4]
        self.log['wind_speed']=data[5]
        self.log['date_time']=(datetime.strptime(str(datetime.utcfromtimestamp(int(data[6])).strftime('%m-%d-%Y %H:%M')),'%m-%d-%Y %H:%M'))
        if(data[8]==data[9].capitalize()):
            self.log['conditions']=data[8]
        else:
            self.log['conditions']=data[8]+", "+data[9].capitalize()
        return self.log



def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        default='gs://weathercalls-bucket/weather-calls*',
        help='Input file to process.')


    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        obj = LoadtoBQ()
        quotes = (
                p
                | 'Read' >> ReadFromText(known_args.input ,skip_header_lines=1)
                | 'Parse Log' >> beam.Map(lambda line: obj.parse(line))
        )

        #quotes | 'Write' >> WriteToText('output')
        quotes | beam.io.gcp.bigquery.WriteToBigQuery(
            obj.table_spec,
            schema=obj.schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location = 'gs://weathercalls-bucket/tmp',
#            method = 'STREAMING_INSERTS'
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()   
