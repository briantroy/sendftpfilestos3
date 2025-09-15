""" Gets the current list of cameras by parsing the s3 bucket objects. """

ALLOWED_ORIGINS = {
    "https://security-videos.brianandkelly.ws",
    "https://sec-vid-dev.brianandkelly.ws",
    "http://localhost:3000"
}

def lambda_handler(event, context):
    import boto3
    import time
    import json

    # Get the Origin header (handle both possible capitalizations)
    origin = event.get('headers', {}).get('origin') or event.get('headers', {}).get('Origin')

    cors_headers = {
        'Access-Control-Allow-Credentials': 'true',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Vary': 'Origin'
    }

    # Only echo allowed origins
    if origin in ALLOWED_ORIGINS:
        print("Origin allowed: " + str(origin))
        cors_headers['Access-Control-Allow-Origin'] = origin
    else:
        print("Origin not allowed: " + str(origin))

    # Figure out HTTP method (works with both HTTP API and REST API event shapes)
    method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')

    # Handle preflight OPTIONS request
    if method == 'OPTIONS':
        return {
            'statusCode': 204,
            'headers': cors_headers,
            'body': ''
        }

    start_time = time.time()
    days_since_last_video = 120

    camera_list = get_s3_camera_metadata()
    print("Camrea List retrieved from S3 in:--- %s seconds ---" % (time.time() - start_time))
    out_list = []

    for camera in camera_list['camera-last-video']:
        if float(camera_list['camera-last-video'][camera]) > (time.time() - ((days_since_last_video)*24*60*60)):
            out_list.append(camera)
        # end If
    # end for

    return_obj = {
        "cameras": out_list,
        "filters": camera_list['filters']

    }

    return {
        'statusCode': 200,
        'headers': cors_headers,
        'body': out_list
    }

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
