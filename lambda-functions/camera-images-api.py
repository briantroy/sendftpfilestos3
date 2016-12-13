""" API endpoint handler to load videos """
from __future__ import print_function

import time
import boto3
from boto3.dynamodb.conditions import Key


def lambda_handler(event, context):
    """ Lambda Handler """
    # print(event)

    dyndb = boto3.resource('dynamodb')

    image_date = None
    num_results = 10
    older_than_ts = None
    newer_than_ts = None
    camera_name = None

    if 'querystring' in event['params']:
        if 'image_date' in event['params']['querystring']:
            image_date = event['params']['querystring']['image_date']
        # Fin
        if 'num_results' in event['params']['querystring']:
            num_results = int(event['params']['querystring']['num_results'])
        # Fin
        if 'older_than_ts' in event['params']['querystring']:
            older_than_ts = int(event['params']['querystring']['older_than_ts'])
        # Fin
        if 'newer_than_ts' in event['params']['querystring']:
            newer_than_ts = int(event['params']['querystring']['newer_than_ts'])
            # Fin
    # Fin
    if 'camera' in event['params']['path']:
        camera_name = event['params']['path']['camera']
    # Fin

    if camera_name is not None:
        print("Request for camera image timeline - Camera: " + camera_name)
    else:
        # Must be a image timeline request

        # defaults:
        if image_date is None:
            image_date = time.strftime('%Y-%m-%d')
        # Fin

        print("Request for image timeline - Date: " + image_date)
    # Fin

    response = execute_dynamo_query(image_date, num_results, older_than_ts, newer_than_ts, camera_name)

    return enrich_image_data(response)


def execute_dynamo_query(image_date, num_results, older_than_ts, newer_than_ts, camera_name):
    """ Generates the correct DynamoDB Query based on input and returns the result.

    :param image_date:
    :param num_results:
    :param older_than_ts:
    :param newer_than_ts:
    :return:
    """

    dyndb = boto3.resource('dynamodb')
    response = None

    if camera_name is not None and image_date is None:
        # Request for camera timeline
        vid_table = dyndb.Table('security_alarm_images')

        if older_than_ts is None and newer_than_ts is None:
            # No timestamp provided to scroll back/forward - get most recent
            response = vid_table.query(
                Select='ALL_ATTRIBUTES',
                KeyConditionExpression=Key('camera_name').eq(camera_name),
                ScanIndexForward=False,
                Limit=num_results,
            )
            return response
        else:
            if older_than_ts is not None:
                # Get items older than timestamp
                response = vid_table.query(
                    Select='ALL_ATTRIBUTES',
                    KeyConditionExpression=Key('camera_name').eq(camera_name),
                    ScanIndexForward=False,
                    Limit=num_results,
                    ExclusiveStartKey={'camera_name': camera_name, 'event_ts': older_than_ts},
                )
                return response
            elif newer_than_ts is not None:
                # Get items newer than timestamp
                response = vid_table.query(
                    Select='ALL_ATTRIBUTES',
                    KeyConditionExpression=Key('camera_name').eq(camera_name),
                    ScanIndexForward=False,
                    Limit=num_results,
                    ExclusiveStartKey={'camera_name': camera_name, 'event_ts': newer_than_ts},
                )
                return response
            # Fin
        # Fin
    # Fin

    if camera_name is None and image_date is not None:
        # Timeline request without regard for camera
        vid_table = dyndb.Table('security_image_timeline')
        if older_than_ts is None and newer_than_ts is None:
            response = vid_table.query(
                Select='ALL_ATTRIBUTES',
                KeyConditionExpression=Key('capture_date').eq(image_date),
                ScanIndexForward=False,
                Limit=num_results,
            )
            return response
        else:
            if older_than_ts is not None:
                # Get items older than timestamp
                response = vid_table.query(
                    Select='ALL_ATTRIBUTES',
                    KeyConditionExpression=Key('capture_date').eq(image_date),
                    ScanIndexForward=False,
                    Limit=num_results,
                    ExclusiveStartKey={'capture_date': image_date, 'event_ts': older_than_ts},
                )
                return response
            elif newer_than_ts is not None:
                # Get items newer than timestamp
                response = vid_table.query(
                    Select='ALL_ATTRIBUTES',
                    KeyConditionExpression=Key('capture_date').eq(image_date),
                    ScanIndexForward=False,
                    Limit=num_results,
                    ExclusiveStartKey={'capture_date': image_date, 'event_ts': newer_than_ts},
                )
                return response
            # Fin
        # Fin

    return response


def generate_signed_uri(item):
    """ Generates signed URIs for the videos - allowing app to load them.

    :param data: List of videos for which signed URIs will be generated.
    :return: The updated data with signed URIs
    """

    s3_client = boto3.client('s3')
    bucket = "security-alarms"

    url = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket,
            'Key': item['object_key']
        }
    )

    return url


def get_image_label(object_key):
    """
    Gets the image labels for an image.

    :param object_key:
    :return:
    """
    dyndb = boto3.resource('dynamodb')

    label_table = dyndb.Table('security_alarm_image_label_set')
    response = label_table.query(
        Select='SPECIFIC_ATTRIBUTES',
        ProjectionExpression='label,confidence',
        KeyConditionExpression=Key('object_key').eq(object_key),
        ScanIndexForward=False,
    )

    return response['Items']


def enrich_image_data(data):
    """
    Enriches the image data as needed.
    :param data:
    :return:
    """
    new_items = []
    for item in data['Items']:
        item['uri'] = generate_signed_uri(item)
        # item['labels'] = get_image_label(item['object_key'])
        new_items.append(item)

    data['Items'] = new_items

    return data
