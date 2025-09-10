""" Lambda function to handle adding new video files to s3 """
from __future__ import print_function

import urllib
import time
import boto3
import json

# For thumbnail generation
import os
import tempfile
import subprocess


def lambda_handler(event, context):
    """ Lambda Handler """

    do_transcode = False

    # print("Received event: " + json.dumps(event, indent=2))
    start_time = time.time()
    # Get the object from the event and show its content type
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key']).encode('utf8')
    size = event['Records'][0]['s3']['object']['size']
    print("Security Video: " + key.decode() + " with size: " + str(size) + " uploaded.")
    # print("Saving Data to DynamoDB")
    object_parts = key.decode().split("/")
    camera_name = object_parts[1]
    # print("Camera Name: " + camera_name)
    # Generate and upload thumbnail using ffmpeg
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    video_key = key.decode()
    thumbnail_object = ""
    thumbnail_object = generate_and_upload_thumbnail_ffmpeg(bucket_name, video_key)
    if "-small.mp4" not in key.decode():

        small_vid_key = key.decode().replace(".mp4", "-small.mp4")

        if do_transcode:
            # Transcode the file for small screens:
            transcoder = boto3.client('elastictranscoder')
            transcoder.create_job(
                PipelineId='1472321641566-68ryf2',
                Input={
                    'Key': key.decode(),
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
            small_vid_key = key.decode()
        # fin

        # Get Object Metadata
        event_ts = time.time()
        obj_metadata = get_s3_metadata(key.decode())
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
                     'object_key': key.decode(),
                     'object_key_small': small_vid_key,
                     'thumbnail_key': thumbnail_object
                    }

        vid_table.put_item(Item=save_data)
        vid_timeline_table.put_item(Item=save_data)

        # update camera metadata
        camera_info = get_s3_camera_metadata()
        camera_info['camera-last-video'][camera_name] = str(int(int(event_ts) / 1000))
        put_s3_camera_metadata(camera_info)
        print("Processing for " + key.decode() + " completed in: " + str(time.time() - start_time) +
              " seconds.")
    else:
        print("Processing for " + key.decode() + " skipped - this is our transcoded file.")
    # fin


def generate_and_upload_thumbnail_ffmpeg(bucket_name, video_key):
    """
    Downloads the video from S3, generates a thumbnail from the first frame using ffmpeg,
    and uploads it to the video-thumbnail/ folder in the same bucket.
    """
    s3 = boto3.client('s3')
    thumb_key = None
    with tempfile.TemporaryDirectory() as tmpdir:
        video_filename = os.path.join(tmpdir, os.path.basename(video_key))
        s3.download_file(bucket_name, video_key, video_filename)

        # Generate thumbnail using ffmpeg (first frame)
        thumb_filename = os.path.splitext(os.path.basename(video_key))[0] + '.jpg'
        thumb_path = os.path.join(tmpdir, thumb_filename)
        ffmpeg_cmd = [
            'ffmpeg',
            '-i', video_filename,
            '-vf', 'thumbnail',
            '-frames:v', '1',
            thumb_path
        ]
        try:
            subprocess.run(ffmpeg_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            print(f"ffmpeg failed: {e.stderr.decode()}")
            return

        # Upload thumbnail to S3 in video-thumbnail/ folder
        thumb_key = f"video-thumbnail/{thumb_filename}"
        s3.upload_file(thumb_path, bucket_name, thumb_key, ExtraArgs={'ContentType': 'image/jpeg'})
        print(f"Thumbnail uploaded to {thumb_key}")

    return thumb_key



def get_s3_metadata(object_key):
    bucket_name = "security-alarms"
    s3_resource = boto3.resource('s3')

    response = s3_resource.ObjectSummary(bucket_name, object_key)
    resp_obj = response.get()

    return resp_obj['Metadata']

def get_s3_camera_metadata():
    bucket_name = "security-alarms-metadata"
    metadata_file = "camera-info.json"
    s3_resource = boto3.resource('s3')

    content_object = s3_resource.Object(bucket_name, metadata_file)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content

def put_s3_camera_metadata(camera_info):
    bucket_name = "security-alarms-metadata"
    metadata_file = "camera-info.json"
    s3_resource = boto3.resource('s3')

    s3_resource.Object(bucket_name, metadata_file).put(Body=json.dumps(camera_info))
