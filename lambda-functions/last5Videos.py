""" API endpoint handler to load videos """
from __future__ import print_function

import time
import boto3
from boto3.dynamodb.conditions import Key
# import pprint


def lambda_handler(event, context):
    """ Lambda Handler """
    # print(event)

    dyndb = boto3.resource('dynamodb')

    # defaults:
    num_results = 10
    video_date = time.strftime('%Y-%m-%d')
    start_key = ""
    by_camera = False
    older_than_ts = 0
    newer_than_ts = 0
    use_ts = 0

    if 'camera' in event['params']['path']:
        camera_name = event['params']['path']['camera']
        key_condition = Key('camera_name').eq(camera_name)
        # print("Request for camera video timeline - Camera: " + camera_name)
        vid_table = dyndb.Table('security_alarm_videos')
        table_name = "security_alarm_videos"
        select_attribs = 'ALL_ATTRIBUTES'
        index_forward = False
        by_camera = True
    else:
        vid_table = dyndb.Table('security_video_timeline')
        table_name = "security_video_timeline"
        select_attribs = 'ALL_ATTRIBUTES'
        key_condition = Key('capture_date').eq(video_date)
        index_forward = False
        by_camera = False
    # Fin
    if 'querystring' in event['params']:
        if 'video_date' in event['params']['querystring']:
            video_date = event['params']['querystring']['image_date']
            key_condition = Key('capture_date').eq(video_date)
        # Fin
        if 'num_results' in event['params']['querystring']:
            num_results = int(event['params']['querystring']['num_results'])
        # Fin
        if 'older_than_ts' in event['params']['querystring']:
            older_than_ts = int(event['params']['querystring']['older_than_ts'])
            use_ts = older_than_ts
            index_forward = False
        # Fin
        if 'newer_than_ts' in event['params']['querystring']:
            newer_than_ts = int(event['params']['querystring']['newer_than_ts'])
            use_ts = newer_than_ts
            index_forward = True
        # Fin
    # Fin

    if by_camera:
        if use_ts > 0:
            start_key = {'camera_name': camera_name, 'event_ts': use_ts}
            response = vid_table.query(
                TableName=table_name,
                Select=select_attribs,
                KeyConditionExpression=key_condition,
                ScanIndexForward=index_forward,
                Limit=num_results,
                ExclusiveStartKey=start_key,
            )
        else:
            response = vid_table.query(
                TableName=table_name,
                Select=select_attribs,
                KeyConditionExpression=key_condition,
                ScanIndexForward=index_forward,
                Limit=num_results,
            )
    else:
        if use_ts > 0:
            start_key = {'capture_date': video_date, 'event_ts': use_ts}
            response = vid_table.query(
                TableName=table_name,
                Select=select_attribs,
                KeyConditionExpression=key_condition,
                ScanIndexForward=index_forward,
                Limit=num_results,
                ExclusiveStartKey=start_key,
            )
        else:
            response = vid_table.query(
                TableName=table_name,
                Select=select_attribs,
                KeyConditionExpression=key_condition,
                ScanIndexForward=index_forward,
                Limit=num_results,
            )
    # FIN

    return generate_signed_uri(response)


def generate_signed_uri(data):
    """ Generates signed URIs for the videos - allowing app to load them.

    :param data: List of videos for which signed URIs will be generated.
    :return: The updated data with signed URIs
    """

    s3_client = boto3.client('s3')
    bucket = "security-alarms"
    new_items = []

    for item in data['Items']:
        url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket,
                'Key': item['object_key']
            }
        )
        item['uri'] = url
        if 'object_key_small' in item:
            url = s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket,
                    'Key': item['object_key_small']
                }
            )
            item['uri_small_video'] = url
        # fin
        new_items.append(item)

    # end for

    data['Items'] = new_items

    return data


""" MOCK for Testing 
mock_event = {'params': {}}
mock_event['params']['path'] = {}
mock_event['params']['path']['camera'] = 'drivewayc1'
mock_event['params']['querystring'] = {}
mock_event['params']['querystring']['older_than_ts'] = 1595518288

output = lambda_handler(mock_event, "")
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(output)
"""
