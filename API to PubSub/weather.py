import requests
import base64
import json
import csv

from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import requests_cache

import os
from google.cloud import pubsub_v1
from concurrent import futures

from flask import Flask

class weather():
	def __init__ (self):
		credentials = '../weatherpubsubkey.json'
		os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
		self.project_id = "gcptraining-319415"
		self.topic_id = "weathercalls"
		self.publisher = pubsub_v1.PublisherClient()
		self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
		self.publish_futures = []

	def get_weather_api(self):
	        try:
	          response = requests.get("https://api.openweathermap.org/data/2.5/weather?id=4930956&units=metric&&appid=")
	          data = json.loads(response.text)
	          requests_cache.install_cache()
	          #print(data)
	          return (data)
	        
	        except (ConnectionError, Timeout, TooManyRedirects) as e:
	          print(e)

	def publish_messages(self, data):
	    """Publishes multiple messages to a Pub/Sub topic with an error handler.."""

	    def get_callback(publish_future, data):
	        def callback(publish_future):
	            try:
	                # Wait 60 seconds for the publish call to succeed.
	                print(publish_future.result(timeout=60))
	            except futures.TimeoutError:
	                print(f"Publishing {data} timed out.")

	        return callback

	    # When you publish a message, the client returns a future.
	    publish_future = self.publisher.publish(self.topic_path, json.dumps(data).encode("utf-8"))
	    # Non-blocking. Publish failures are handled in the callback function.

	    publish_future.add_done_callback(get_callback(publish_future, data))
	    self.publish_futures.append(publish_future)

	    print(f"Published messages to {self.topic_path}.{data}")
	    
if __name__ == '__main__':
	serv = weather()
	message=serv.get_weather_api()
	serv.publish_messages(message)
