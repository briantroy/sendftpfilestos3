""" Lambda function to handle adding new video files to s3 """
from __future__ import print_function

import urllib
import time
import boto3


def lambda_handler(event, context):
    """ Lambda Handler """

    # print("Received event: " + json.dumps(event, indent=2))
    start_time = time.time()
    # Get the object from the event and show its content type
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).encode('utf8')
    size = event['Records'][0]['s3']['object']['size']
    print("Security Image: " + key + " with size: " + str(size) + " uploaded.")
    # print("Saving Data to DynamoDB")
    object_parts = key.split("/")
    camera_name = object_parts[1]
    # print("Camera Name: " + camera_name)

    dyndb = boto3.resource('dynamodb')
    img_table = dyndb.Table('security_alarm_images')
    img_timeline_table = dyndb.Table('security_image_timeline')
    save_data = {'camera_name': camera_name,
                 'image_size': size,
                 'image_name': object_parts[5],
                 'capture_date': object_parts[2],
                 'capture_hour': object_parts[3],
                 'event_ts': int(time.time()),
                 'object_key': key
                }

    img_table.put_item(Item=save_data)
    img_timeline_table.put_item(Item=save_data)

    print("Processing for " + key + " completed in: " + str(time.time() - start_time) +
          " seconds.")

