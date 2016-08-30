""" API endpoint handler to load videos """
from __future__ import print_function

import time
import boto3
from boto3.dynamodb.conditions import Key


def lambda_handler(event, context):
    """ Lambda Handler """
    print(event)

    dyndb = boto3.resource('dynamodb')

    if 'camera' in event['params']['path']:
        camname = event['params']['path']['camera']
        vid_table = dyndb.Table('security_alarm_videos')
        response = vid_table.query(
            TableName="security_alarm_videos",
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=Key('camera_name').eq(camname),
            ScanIndexForward=False,
            Limit=5,
        )
    else:

        # Must be a video timeline request

        # defaults:
        num_results = 10
        video_date = time.strftime('%Y-%m-%d')
        print(video_date)

        if 'querystring' in event['params']:
            if 'video_date' in event['params']['querystring']:
                video_date = event['params']['querystring']['video_date']
            # Fin
            if 'num_results' in event['params']['querystring']:
                num_results = int(event['params']['querystring']['num_results'])
            # Fin
        # Fin

        # Execute the query

        vid_table = dyndb.Table('security_alarm_videos')
        response = vid_table.query(
            TableName="security_video_timeline",
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=Key('capture_date').eq(video_date),
            ScanIndexForward=False,
            Limit=num_results,
        )
    # Fin

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
