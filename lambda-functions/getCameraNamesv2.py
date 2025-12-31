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

    # Handle POST request to update camera metadata
    if method == 'POST':
        try:
            # Parse the request body
            body = event.get('body', '{}')
            if isinstance(body, str):
                incoming_data = json.loads(body)
            else:
                incoming_data = body

            # Validate the incoming JSON structure
            if not isinstance(incoming_data, dict):
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'Request body must be a JSON object'})
                }

            # Validate that camera-last-video is not in incoming data (should only come from S3)
            if 'camera-last-video' in incoming_data:
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'camera-last-video cannot be set via POST request'})
                }

            # Validate required fields are present
            if 'cameras' not in incoming_data:
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'Missing required field: cameras'})
                }

            if 'filters' not in incoming_data:
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'Missing required field: filters'})
                }

            # Validate cameras field type
            if not isinstance(incoming_data['cameras'], list):
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'cameras must be an array'})
                }

            # Validate filters field type
            if not isinstance(incoming_data['filters'], dict):
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'filters must be an object'})
                }

            # Validate each filter has the required structure
            for filter_name, filter_def in incoming_data['filters'].items():
                if not isinstance(filter_def, dict):
                    return {
                        'statusCode': 400,
                        'headers': cors_headers,
                        'body': json.dumps({'error': f'Filter "{filter_name}" must be an object'})
                    }
                if 'operator' not in filter_def:
                    return {
                        'statusCode': 400,
                        'headers': cors_headers,
                        'body': json.dumps({'error': f'Filter "{filter_name}" missing required field: operator'})
                    }
                if 'value' not in filter_def:
                    return {
                        'statusCode': 400,
                        'headers': cors_headers,
                        'body': json.dumps({'error': f'Filter "{filter_name}" missing required field: value'})
                    }
                # Validate operator values
                valid_operators = ['contains', 'not_contains', 'in']
                if filter_def['operator'] not in valid_operators:
                    return {
                        'statusCode': 400,
                        'headers': cors_headers,
                        'body': json.dumps({'error': f'Filter "{filter_name}" has invalid operator. Must be one of: {", ".join(valid_operators)}'})
                    }

            # Get existing camera metadata from S3
            existing_data = get_s3_camera_metadata()

            # Start with incoming data (overwrites existing)
            merged_data = incoming_data.copy()

            # Preserve camera-last-video from existing data in S3
            merged_data['camera-last-video'] = existing_data.get('camera-last-video', {})

            # Write the merged data to S3
            put_s3_camera_metadata(merged_data)

            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Camera metadata updated successfully'})
            }

        except json.JSONDecodeError as e:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': f'Invalid JSON: {str(e)}'})
            }
        except Exception as e:
            print(f"Error processing POST request: {str(e)}")
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Internal server error'})
            }

    # Handle GET request (existing functionality)
    start_time = time.time()
    days_since_last_video = 120

    camera_list = get_s3_camera_metadata()
    print("Camrea List retrieved from S3 in:--- %s seconds ---" % (time.time() - start_time))

    # Get cameras that have videos in the last X days
    active_cameras = set()
    for camera in camera_list['camera-last-video']:
        if float(camera_list['camera-last-video'][camera]) > (time.time() - ((days_since_last_video)*24*60*60)):
            active_cameras.add(camera)
        # end If
    # end for

    # Merge with user's ordered camera list
    user_ordered_cameras = camera_list.get('cameras', [])
    out_list = []

    # Add cameras from user's ordered list that are still active
    for camera in user_ordered_cameras:
        if camera in active_cameras:
            out_list.append(camera)
            active_cameras.remove(camera)  # Track that we've added this camera

    # Add any new cameras (in camera-last-video but not in user's ordered list) at the end
    for camera in sorted(active_cameras):  # Sort for consistent ordering of new cameras
        out_list.append(camera)

    return_obj = {
        "cameras": out_list,
        "filters": camera_list['filters']

    }

    return {
        'statusCode': 200,
        'headers': cors_headers,
        'body': json.dumps(return_obj)
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
