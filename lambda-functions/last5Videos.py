from __future__ import print_function

import boto3
import time
from boto3.dynamodb.conditions import Key, Attr


def lambda_handler(event, context):
    print(event)

    dyndb = boto3.resource('dynamodb')
    response = {"result": "Invalid Request"}
    good_request = False

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
        good_reqeust = True
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

    return generate_signed_uri_for_resposne(response)


def generate_signed_uri_for_resposne(data):

    s3 = boto3.client('s3')
    bucket = "security-alarms"
    new_items = []

    for item in data['Items']:
        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket,
                'Key': item['object_key']
            }
        )
        item['uri'] = url
        new_items.append(item)

    # end for

    data['Items'] = new_items

    return data
