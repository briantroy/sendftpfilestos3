""" API endpoint handler to load videos """
from __future__ import print_function

import time
import boto3
from boto3.dynamodb.conditions import Key


def lambda_handler(event, context):
    """ Lambda Handler """
    # print(event)

    dyndb = boto3.resource('dynamodb')

    if 'camera' in event['params']['path']:
        camname = event['params']['path']['camera']
        print("Request for camera image timeline - Camera: " + camname)
        vid_table = dyndb.Table('security_alarm_images')
        response = vid_table.query(
            TableName="security_alarm_images",
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=Key('camera_name').eq(camname),
            ScanIndexForward=False,
            Limit=10,
        )
    else:

        # Must be a image timeline request

        # defaults:
        num_results = 10
        image_date = time.strftime('%Y-%m-%d')
        # print(image_date)
        print("Request for image timeline - Date: " + image_date)

        if 'querystring' in event['params']:
            if 'image_date' in event['params']['querystring']:
                image_date = event['params']['querystring']['image_date']
            # Fin
            if 'num_results' in event['params']['querystring']:
                num_results = int(event['params']['querystring']['num_results'])
            # Fin
        # Fin

        # Execute the query

        vid_table = dyndb.Table('security_image_timeline')
        response = vid_table.query(
            TableName="security_image_timeline",
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=Key('capture_date').eq(image_date),
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
        new_items.append(item)

    # end for

    data['Items'] = new_items

    return data
