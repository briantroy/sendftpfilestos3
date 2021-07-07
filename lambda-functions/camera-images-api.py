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
