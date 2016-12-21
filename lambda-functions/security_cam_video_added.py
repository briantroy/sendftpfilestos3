""" Lambda function to handle adding new video files to s3 """
from __future__ import print_function

import urllib
import time
import boto3


def lambda_handler(event, context):
    """ Lambda Handler """

    do_transcode = False
    # print("Received event: " + json.dumps(event, indent=2))
    start_time = time.time()
    # Get the object from the event and show its content type
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).encode('utf8')
    size = event['Records'][0]['s3']['object']['size']
    print("Security Video: " + key + " with size: " + str(size) + " uploaded.")
    # print("Saving Data to DynamoDB")
    object_parts = key.split("/")
    camera_name = object_parts[1]
    # print("Camera Name: " + camera_name)

    if "-small.mp4" not in key:

        small_vid_key = key.replace(".mp4", "-small.mp4")

        if do_transcode:
            # Transcode the file for small screens:
            transcoder = boto3.client('elastictranscoder')
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
        else:
            small_vid_key = key
        # fin

        # Get Object Metadata
        event_ts = time.time()
        obj_metadata = get_s3_metadata(key)
        if 'camera_timestamp' in obj_metadata:
            event_ts = obj_metadata['camera_timestamp']
            # FIN

        dyndb = boto3.resource('dynamodb')
        vid_table = dyndb.Table('security_alarm_videos')
        vid_timeline_table = dyndb.Table('security_video_timeline')
        save_data = {'camera_name': camera_name,
                     'video_size': size,
                     'video_name': object_parts[5],
                     'capture_date': object_parts[2],
                     'capture_hour': object_parts[3],
                     'event_ts': int(event_ts),
                     's3_arrival_time': int(time.time()),
                     'object_key': key,
                     'object_key_small': small_vid_key
                    }

        vid_table.put_item(Item=save_data)
        vid_timeline_table.put_item(Item=save_data)

        print("Processing for " + key + " completed in: " + str(time.time() - start_time) +
              " seconds.")
    else:
        print("Processing for " + key + " skipped - this is our transcoded file.")
    # fin


def get_s3_metadata(object_key):
    bucket_name = "security-alarms"
    s3_resource = boto3.resource('s3')

    response = s3_resource.ObjectSummary(bucket_name, object_key)
    resp_obj = response.get()

    return resp_obj['Metadata']