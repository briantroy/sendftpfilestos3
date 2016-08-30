""" Gets the current list of cameras by parsing the s3 bucket objects. """
def lambda_handler(event, context):
    """ Lambda Hander """
    import boto3

    s3_client = boto3.client('s3')
    items = s3_client.list_objects(Bucket='security-alarms', Prefix='patrolcams/', Delimiter='/')

    out_list = []
    for obj in items.get('CommonPrefixes', []):
        camera = obj.get('Prefix')
        camera = camera.replace('patrolcams/', '')
        camera = camera.replace('/', '')
        out_list.append(camera)
        # end for

    return out_list
