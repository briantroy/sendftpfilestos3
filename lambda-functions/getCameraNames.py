""" Gets the current list of cameras by parsing the s3 bucket objects. """
def lambda_handler(event, context):
    """ Lambda Hander """
    import boto3
    from boto3.dynamodb.conditions import Key
    import time

    dyndb = boto3.resource('dynamodb')
    days_since_last_video = 120

    s3_client = boto3.client('s3')
    items = s3_client.list_objects(Bucket='security-alarms', Prefix='patrolcams/', Delimiter='/')
    print("Request for camera list.")
    out_list = []
    for obj in items.get('CommonPrefixes', []):
        camera = obj.get('Prefix')
        camera = camera.replace('patrolcams/', '')
        camera = camera.replace('/', '')
        
        # See if we have videos in the last 6 months for this camera
        vid_table = dyndb.Table('security_video_timeline')
        table_name = "security_alarm_videos"
        select_attribs = 'ALL_ATTRIBUTES'
        index_forward = False
        key_condition = Key('camera_name').eq(camera)
        
        response = vid_table.query(
            TableName=table_name,
            Select=select_attribs,
            ScanIndexForward=index_forward,
            KeyConditionExpression=key_condition,
            Limit=1,
        )
        for item in response['Items']:
            if item['event_ts']/1000 > (time.time() - ((days_since_last_video)*24*60*60)):
                out_list.append(camera)
            # end If
            break
        # end for
    # end for

    return out_list
