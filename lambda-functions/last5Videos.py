from __future__ import print_function

import boto3
from boto3.dynamodb.conditions import Key, Attr


def lambda_handler(event, context):
    camname = event['params']['path']['camera']
    dyndb = boto3.resource('dynamodb')
    vid_table = dyndb.Table('security_alarm_videos')
    # cameraname = event.params.path.camera
    response = vid_table.query(
        TableName="security_alarm_videos",
        Select='ALL_ATTRIBUTES',
        KeyConditionExpression=Key('camera_name').eq(camname),
        ScanIndexForward=False,
        Limit=5,
    )
    return(response)