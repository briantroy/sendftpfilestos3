""" API endpoint handler to load image labels """
from __future__ import print_function

import boto3
import json
from boto3.dynamodb.conditions import Key
from decimal import Decimal


class DecimalEncoder(json.JSONEncoder):
    """Helper class to convert DynamoDB Decimals to int/float for JSON serialization."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        return super(DecimalEncoder, self).default(obj)


def lambda_handler(event, context):
    """ Lambda Handler for API Gateway Proxy Integration """

    dyndb = boto3.resource('dynamodb')

    # Parse query string parameters safely for proxy integration
    query = event.get('queryStringParameters') or {}
    image_key = query.get('image-key')

    if image_key:
        print("Request for camera image labels - Image: " + image_key)
        label_table = dyndb.Table('security_alarm_image_label_set')
        response = label_table.query(
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression='label,confidence',
            KeyConditionExpression=Key('object_key').eq(image_key),
            ScanIndexForward=False,
        )
        items = response.get('Items', [])
    else:
        items = []

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
        "body": json.dumps(items, cls=DecimalEncoder)
    }
