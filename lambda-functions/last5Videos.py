""" API endpoint handler to load videos """
from __future__ import print_function

import time
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

# import pprint


def lambda_handler(event, context):
    """ Lambda Handler """
    # print(event)
    print("Version: pagination version")

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
        print("Request for camera video timeline - Camera: " + camera_name)
        vid_table = dyndb.Table('security_alarm_videos')
        table_name = "security_alarm_videos"
        select_attribs = 'ALL_ATTRIBUTES'
        index_forward = False
        by_camera = True
    else:
        vid_table = dyndb.Table('security_video_timeline')
        print("Request for camera video timeline: " + video_date)
        table_name = "security_video_timeline"
        select_attribs = 'ALL_ATTRIBUTES'
        key_condition = Key('capture_date').eq(video_date)
        index_forward = False
        by_camera = False
    # Fin
    if 'querystring' in event['params']:
        if 'video_date' in event['params']['querystring']:
            video_date = event['params']['querystring']['video_date']
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

    filter_expression = False
    if 'filter' in event['params']['querystring']:
        filter_name = event['params']['querystring']['filter']
        # get camera metadata with filters
        camera_metadata = get_s3_camera_metadata()
        filter_list = camera_metadata['filters']
        with filter_list[filter_name] as this_filter:
            if this_filter['operator'] == 'contains':
                filter_expression = Attr('camera_name').contains(this_filter['value'])
            if this_filter['operator'] == 'not_contains':
                filter_expression = ~Attr('camera_name').contains(this_filter['value'])
            if this_filter['operator'] == 'in':
                filter_expression = Attr('camera_name').is_in(this_filter['value'])
        # get a lot of results... because filtering happens after the limit.
        num_results = 200

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
        if filter_expression:
            if use_ts > 0:
                start_key = {'capture_date': video_date, 'event_ts': use_ts}
                response = vid_table.query(
                    TableName=table_name,
                    Select=select_attribs,
                    KeyConditionExpression=key_condition,
                    FilterExpression=filter_expression,
                    Limit=num_results,
                    ScanIndexForward=index_forward,
                    ExclusiveStartKey=start_key,
                )
            else:
                response = vid_table.query(
                    TableName=table_name,
                    Select=select_attribs,
                    KeyConditionExpression=key_condition,
                    FilterExpression=filter_expression,
                    Limit=num_results,
                    ScanIndexForward=index_forward,
                )
            # FIN
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
        # FIN
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

def get_s3_camera_metadata():
    import boto3
    import json
    bucket_name = "security-alarms-metadata"
    metadata_file = "camera-info.json"
    s3_resource = boto3.resource('s3')

    content_object = s3_resource.Object(bucket_name, metadata_file)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content