""" API endpoint handler to load images """
from __future__ import print_function

import time
import boto3
import json
from boto3.dynamodb.conditions import Key
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    """Helper class to convert DynamoDB Decimals to int/float for JSON serialization."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Convert to int if possible, else float
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    """ Lambda Handler for API Gateway Proxy Integration """

    dyndb = boto3.resource('dynamodb')

    # Parse query string and path parameters safely
    query = event.get('queryStringParameters') or {}
    path = event.get('pathParameters') or {}

    image_date = query.get('image_date')
    num_results = int(query.get('num_results', 10))
    older_than_ts = query.get('older_than_ts')
    newer_than_ts = query.get('newer_than_ts')
    camera_name = path.get('camera')

    if older_than_ts is not None:
        try:
            older_than_ts = int(older_than_ts)
        except ValueError:
            older_than_ts = None
    if newer_than_ts is not None:
        try:
            newer_than_ts = int(newer_than_ts)
        except ValueError:
            newer_than_ts = None

    if camera_name:
        print("Request for camera image timeline - Camera: " + camera_name)
    else:
        if image_date is None:
            image_date = time.strftime('%Y-%m-%d')
        print("Request for image timeline - Date: " + image_date)

    response = execute_dynamo_query(image_date, num_results, older_than_ts, newer_than_ts, camera_name)
    enriched = enrich_image_data(response)

    # CORS headers (customize as needed)
    allowed_origins = [
        "https://security-videos.brianandkelly.ws",
        "https://sec-vid-dev.brianandkelly.ws",
        "http://localhost:3000"
    ]
    origin = event.get('headers', {}).get('origin')
    if origin in allowed_origins:
        print("Origin allowed: " + str(origin))
        cors_headers = {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
        }
    else:
        print("Origin not allowed: " + str(origin)) 
        cors_headers = {}

    return {
        "statusCode": 200,
        "headers": cors_headers,
        "body": json.dumps(enriched, cls=DecimalEncoder)
    }

def execute_dynamo_query(image_date, num_results, older_than_ts, newer_than_ts, camera_name):
    """ Generates the correct DynamoDB Query based on input and returns the result. """

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
                    ScanIndexForward=True,
                    Limit=num_results,
                    ExclusiveStartKey={'camera_name': camera_name, 'event_ts': newer_than_ts},
                )
                return response

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
                    ScanIndexForward=True,
                    Limit=num_results,
                    ExclusiveStartKey={'capture_date': image_date, 'event_ts': newer_than_ts},
                )
                return response

    return response

def generate_signed_uri(item):
    """ Generates signed URIs for the videos - allowing app to load them. """

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
    """ Gets the image labels for an image. """
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
    """ Enriches the image data as needed. """
    new_items = []
    for item in data['Items']:
        item['uri'] = generate_signed_uri(item)
        # item['labels'] = get_image_label(item['object_key'])
        new_items.append(item)

    data['Items'] = new_items

    return data
