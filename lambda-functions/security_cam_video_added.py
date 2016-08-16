from __future__ import print_function

import json
import urllib
import boto3
import time

print('Loading function')



def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).encode('utf8')
    size = event['Records'][0]['s3']['object']['size']
    print("Security Video: " + key + " with size: " + str(size) + " uploaded.")
    print("Saving Data to DynamoDB")
    object_parts = key.split("/")
    camera_name = object_parts[1]
    print("Camera Name: " + camera_name)

    dyndb = boto3.resource('dynamodb')
    vid_table = dyndb.Table('security_alarm_videos')
    save_data = {'camera_name': camera_name,
                    'video_size': size,
                    'video_name': object_parts[5],
                    'capture_date': object_parts[2],
                    'capture_hour': object_parts[3],
                    'event_ts': int(time.time()),
                    'object_key': key
                }
    response = vid_table.put_item(Item=save_data
                                  )
    return size