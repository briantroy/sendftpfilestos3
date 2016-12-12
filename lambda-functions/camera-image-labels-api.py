""" API endpoint handler to load image labels """
from __future__ import print_function

import boto3
from boto3.dynamodb.conditions import Key


def lambda_handler(event, context):
    """ Lambda Handler """
    # print(event)

    dyndb = boto3.resource('dynamodb')

    if 'image-key' in event['params']['querystring']:
        image_key = event['params']['querystring']['image-key']
        print("Request for camera image labels - Image: " + image_key)
        label_table = dyndb.Table('security_alarm_image_label_set')
        response = label_table.query(
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression='label,confidence',
            KeyConditionExpression=Key('object_key').eq(image_key),
            ScanIndexForward=False,
        )

        return response
