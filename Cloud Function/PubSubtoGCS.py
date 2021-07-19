import base64
import logging
import pandas as pd
import json
from google.cloud.storage import Client
import time

class LoadToStorage:
    def __init__(self,event,context):
        self.event=event
        self.context=context
        self.bucket_name='weathercalls-bucket'

    def getMsgData(self) -> str:
        logging.info("Function triggered, retrieving data")
        message_chunk=base64.b64decode(self.event['data']).decode('utf-8')
        logging.info("Datapoint validated")
        return message_chunk
        
    def payloadToDf(self,message:str) -> pd.DataFrame:
        try:
          data = json.loads(message)
          data["new_weather"]= data['weather'][0]
          del data['weather']
          new_json = dict()
          newkey = dict()
          for key in data:
            if(type(data[key]) is dict):
              for key2 in data[key]:
                new_json[key+"_"+key2] = data[key][key2]
            else:
                new_json[key] = data[key]
          print(new_json)
          print(newkey)
          filter_cols = ['main_temp', 'main_temp_min', 'main_temp_max', 'visibility', 'wind_speed', 'dt', 'main_humidity', 'name', 'new_weather_main','new_weather_description']
          for (element, value) in new_json.items():
              if(element in filter_cols):
                  newkey[element] = value
          df=pd.DataFrame(newkey, index=[0])
          print(df)
          if not df.empty:
              logging.info("DF created")
          else:
              logging.info("Empty DF created")
          return df
        except Exception as e:
          logging.error(f"Error creating DF {str(e)}")
          raise

    def uploadToBucket(self,df,filename):
        storage_client=Client()
        bucket=storage_client.bucket(self.bucket_name)
        blob=bucket.blob(f"{filename}.csv")
        blob.upload_from_string(data=df.to_csv(index=False),content_type='text/csv')
        logging.info("File uploaded to bucket")


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    logging.basicConfig(level=logging.INFO)
    service=LoadToStorage(event,context)
    message=service.getMsgData()
    df=service.payloadToDf(message)
    timestamp=str(int(time.time()))
    service.uploadToBucket(df,"weather-calls"+timestamp)
