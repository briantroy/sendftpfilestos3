from __future__ import print_function

import json
import urllib
import boto3
import time




def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    start_time = time.time()
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).encode('utf8')
    size = event['Records'][0]['s3']['object']['size']
    print("Security Video: " + key + " with size: " + str(size) + " uploaded.")
    # print("Saving Data to DynamoDB")
    object_parts = key.split("/")
    camera_name = object_parts[1]
    # print("Camera Name: " + camera_name)

    if "-small.mp4" not in key:

        # Transcode the file for small screens:
        transcoder = boto3.client('elastictranscoder')
        small_vid_key = key.replace(".mp4", "-small.mp4")
        transcoder.create_job(
            PipelineId='1472321641566-68ryf2',
            Input={
                'Key': key,
                'FrameRate': 'auto',
                'Resolution': 'auto',
                'AspectRatio': 'auto',
                'Interlaced': 'auto',
                'Container': 'auto'
            },
            Outputs=[{
                'Key': small_vid_key,
                'PresetId': '1351620000001-000061'
            }]
        )

        dyndb = boto3.resource('dynamodb')
        vid_table = dyndb.Table('security_alarm_videos')
        vid_timeline_table = dyndb.Table('security_video_timeline')
        save_data = {'camera_name': camera_name,
                        'video_size': size,
                        'video_name': object_parts[5],
                        'capture_date': object_parts[2],
                        'capture_hour': object_parts[3],
                        'event_ts': int(time.time()),
                        'object_key': key,
                        'object_key_small': small_vid_key
                    }

        response = vid_table.put_item(Item=save_data)
        response2 = vid_timeline_table.put_item(Item=save_data)

        print("Processing for " + key + " completed in: " + str(time.time() - start_time) + " seconds.")
    else:
        print("Processing for " + key + " skipped - this is our transcoded file.")
    # fin