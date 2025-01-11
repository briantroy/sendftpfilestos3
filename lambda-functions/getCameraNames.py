""" Gets the current list of cameras by parsing the s3 bucket objects. """
def lambda_handler(event, context):
    """ Lambda Hander """
    import boto3
    import time
    import json

    days_since_last_video = 120

    camera_list = get_s3_camera_metadata()
    out_list = []

    for camera in camera_list['camera-last-video']:
        if float(camera_list['camera-last-video'][camera]) > (time.time() - ((days_since_last_video)*24*60*60)):
            out_list.append(camera)
        # end If
    # end for

    return out_list

def get_s3_camera_metadata():
    import boto3
    import json
    bucket_name = "security-alarms-metadata"
    metadata_file = "camera-info.json"
    s3_resource = boto3.resource('s3')

    content_object = s3_resource.Object(bucket_name, metadata_file)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content

def put_s3_camera_metadata(camera_info):
    import boto3
    import json
    bucket_name = "security-alarms-metadata"
    metadata_file = "camera-info.json"
    s3_resource = boto3.resource('s3')

    s3_resource.Object(bucket_name, metadata_file).put(Body=json.dumps(camera_info))
