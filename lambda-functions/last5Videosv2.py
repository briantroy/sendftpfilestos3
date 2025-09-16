""" API endpoint handler to load videos """
from __future__ import print_function

import time
import boto3
import json
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from decimal import Decimal

# import pprint

ALLOWED_ORIGINS = {
    "https://security-videos.brianandkelly.ws",
    "https://sec-vid-dev.brianandkelly.ws",
    "http://localhost:3000"
}


def lambda_handler(event, context):
    """ Lambda Handler """
    print(event)

    # Get the Origin header (handle both possible capitalizations)
    origin = event.get('headers', {}).get('origin') or event.get('headers', {}).get('Origin')

    cors_headers = {
        'Access-Control-Allow-Credentials': 'true',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Vary': 'Origin'
    }

    # Only echo allowed origins
    if origin in ALLOWED_ORIGINS:
        print("Origin allowed: " + str(origin))
        cors_headers['Access-Control-Allow-Origin'] = origin
    else:
        print("Origin not allowed: " + str(origin))
    # end If

    # Figure out HTTP method (works with both HTTP API and REST API event shapes)
    method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')

    # Handle preflight OPTIONS request
    if method == 'OPTIONS':
        return {
            'statusCode': 204,
            'headers': cors_headers,
            'body': ''
        }
    
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

    path_params = event.get('pathParameters') or {}
    if path_params and ('camera' in path_params):
        camera_name = path_params['camera']
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
    qs_params = event.get('queryStringParameters') or {}
    if qs_params:
        if 'video_date' in qs_params:
            video_date = qs_params['video_date']
            key_condition = Key('capture_date').eq(video_date)
        if 'num_results' in qs_params:
            num_results = int(qs_params['num_results'])
        if 'older_than_ts' in qs_params:
            older_than_ts = int(qs_params['older_than_ts'])
            use_ts = older_than_ts
            index_forward = False
        if 'newer_than_ts' in qs_params:
            newer_than_ts = int(qs_params['newer_than_ts'])
            use_ts = newer_than_ts
            index_forward = True

    filter_expression = False
    if qs_params and 'filter' in qs_params:
        filter_name = qs_params['filter']
        # get camera metadata with filters
        camera_metadata = get_s3_camera_metadata()
        print(filter_name)
        print(camera_metadata['filters'])
        this_filter = camera_metadata['filters'][filter_name]
        filter_list = camera_metadata['filters']

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

    output = generate_signed_uri(response)
    
    return {
        'statusCode': 200,
        'headers': cors_headers,
        'body': json.dumps(output, default=int)
    }


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
        if 'thumbnail_key' in item and item['thumbnail_key'] != "":
            url = s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket,
                    'Key': item['thumbnail_key']
                }
            )
            item['thumbnail_uri'] = url
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