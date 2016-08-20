def lambda_handler(event, context):
    import boto3

    s3 = boto3.client('s3')
    items = s3.list_objects(Bucket='security-alarms', Prefix='patrolcams/', Delimiter='/')

    out_list = []
    for obj in items.get('CommonPrefixes', []):
        camera = obj.get('Prefix')
        camera = camera.replace('patrolcams/', '')
        camera = camera.replace('/', '')
        out_list.append(camera)
        # end for

    return out_list