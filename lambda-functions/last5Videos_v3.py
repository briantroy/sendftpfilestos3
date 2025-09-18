""" API endpoint handler to load videos """
from __future__ import print_function

import json
import time
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from boto3.dynamodb.conditions import Attr

ALLOWED_ORIGINS = {
    "https://security-videos.brianandkelly.ws",
    "https://sec-vid-dev.brianandkelly.ws",
    "http://localhost:3000"
}

class DecimalEncoder(json.JSONEncoder):
    """Convert DynamoDB Decimal to int/float for JSON serialization."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)


def lambda_handler(event, context):
    """ Lambda Handler """
    print(event)

    # Get the Origin header (handle both possible capitalizations)
    headers_in = event.get('headers') or {}
    origin = headers_in.get('origin') or headers_in.get('Origin')

    cors_headers = {
        'Access-Control-Allow-Credentials': 'true',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Vary': 'Origin'
    }
    # Only echo allowed origins
    if origin in ALLOWED_ORIGINS:
        cors_headers['Access-Control-Allow-Origin'] = origin

    # Figure out HTTP method (works with both HTTP API and REST API event shapes)
    method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')

    # Handle preflight OPTIONS request
    if method == 'OPTIONS':
        return {
            "statusCode": 204,
            "headers": cors_headers,
            "body": ""
        }

    print("Version: pagination version")

    dyndb = boto3.resource('dynamodb')
    consolidated_response = {
        "Items": [],
        "Count": 0,
        "LastEvaluatedKey": None
    }

    # Parse inputs safely
    qs = event.get('queryStringParameters') or {}
    path = event.get('pathParameters') or {}

    # camera in the path (e.g., /videos/{camera})
    camera_name = (path or {}).get('camera')

    # Only default the date when not querying by camera
    video_date = qs.get('video_date')
    if not camera_name and not video_date:
        video_date = time.strftime('%Y-%m-%d')

    # num_results
    try:
        num_results = int(qs.get('num_results', 10))
    except (TypeError, ValueError):
        num_results = 10

    requested_results = num_results

    # timestamps
    older_than_ts = qs.get('older_than_ts')
    newer_than_ts = qs.get('newer_than_ts')
    try:
        older_than_ts = int(older_than_ts) if older_than_ts is not None else None
    except (TypeError, ValueError):
        older_than_ts = None
    try:
        newer_than_ts = int(newer_than_ts) if newer_than_ts is not None else None
    except (TypeError, ValueError):
        newer_than_ts = None

    filter_expression = None
    if 'filter' in qs:
        filter_name = qs['filter']
        # get camera metadata with filters
        camera_metadata = get_s3_camera_metadata()
        print("Using Filter: ", filter_name)
        this_filter = camera_metadata['filters'][filter_name]
        filter_list = camera_metadata['filters']
    
        if this_filter['operator'] == 'contains':
            filter_expression = Attr('camera_name').contains(this_filter['value'])
        if this_filter['operator'] == 'not_contains':
            filter_expression = ~Attr('camera_name').contains(this_filter['value'])
        if this_filter['operator'] == 'in':
            filter_expression = Attr('camera_name').is_in(this_filter['value'])

    # Fetch from DynamoDB via refactored function
    ddb_response = query_videos(
        dyndb=dyndb,
        camera_name=camera_name,
        video_date=video_date,
        filter_expression=filter_expression,
        num_results=num_results,
        older_than_ts=older_than_ts,
        newer_than_ts=newer_than_ts,
    )

    print("Request Data:")
    print("Camera Name: ", camera_name)
    print("Video Date: ", video_date)
    print("Num Results: ", num_results)
    print("Older Than TS: ", older_than_ts)
    print("Newer Than TS: ", newer_than_ts)
    print("Filter Expression: ", filter_expression)
    request_num = 1
    fetch_more_than_one = True
    if ddb_response['Count'] < requested_results:
        fetch_more_than_one = True
        # don't fetch more if we are getting newer videos
        if newer_than_ts is not None:
            fetch_more_than_one = False
    else:
        fetch_more_than_one = False

    if not fetch_more_than_one:
        consolidated_response = ddb_response


    prev_last_evaluated_key = ddb_response.get('LastEvaluatedKey')
    consolidated_response['Items'].extend(ddb_response.get('Items', []))
    consolidated_response['Count'] += ddb_response.get('Count', 0)
    consolidated_response['DynDBRequests'] = request_num
    left_to_fetch = requested_results - consolidated_response['Count']  
    print("After request #{}: total items {}, LastEvaluatedKey: {}".format(
        request_num, consolidated_response['Count'], ddb_response.get('LastEvaluatedKey')))

    while consolidated_response['Count'] < requested_results and fetch_more_than_one:
        print("Request: {} scanned {} and returned {} items.".format(request_num, ddb_response.get('ScannedCount', 0), ddb_response.get('Count', 0)))
        print(f"Left to fetch: {left_to_fetch}")
        print("ScannedCount last run: ", ddb_response.get('ScannedCount', 0))
        if 'LastEvaluatedKey' in ddb_response and ddb_response['LastEvaluatedKey']:
            print("Fetching more, LastEvaluatedKey: ", ddb_response['LastEvaluatedKey'])
            older_than_ts = ddb_response['LastEvaluatedKey'].get('event_ts') if ddb_response['LastEvaluatedKey'] else None
            print ("New older_than_ts for next query: ", older_than_ts)
            ddb_response = query_videos(
                dyndb=dyndb,
                camera_name=camera_name,
                video_date=video_date,
                filter_expression=filter_expression,
                num_results=left_to_fetch,
                older_than_ts=older_than_ts,
                newer_than_ts=newer_than_ts,
            )
            scanned_last_run=ddb_response.get('ScannedCount', 0)    
        else:
            # No LastEvaluatedKey, decrement 1 day from video_date
            if not camera_name and video_date:
                try:
                    video_time = time.strptime(video_date, '%Y-%m-%d')
                    prev_day = time.localtime(time.mktime(video_time) - 86400)
                    video_date = time.strftime('%Y-%m-%d', prev_day)
                    older_than_ts = None
                    print("No more pages, moving to previous day: ", video_date)
                    ddb_response = query_videos(
                        dyndb=dyndb,
                        camera_name=camera_name,
                        video_date=video_date,
                        filter_expression=filter_expression,
                        num_results=left_to_fetch,
                        older_than_ts=older_than_ts,
                        newer_than_ts=newer_than_ts,
                    )
                    scanned_last_run=ddb_response.get('ScannedCount', 0)
                except ValueError:
                    print("Invalid date format, stopping pagination.")
                    break  
        request_num += 1
        prev_last_evaluated_key = ddb_response.get('LastEvaluatedKey')
        consolidated_response['Items'].extend(ddb_response.get('Items', []))
        consolidated_response['Count'] += ddb_response.get('Count', 0)
        consolidated_response['DynDBRequests'] = request_num
        left_to_fetch = requested_results - consolidated_response['Count']  
        print("After request #{}: total items {}, LastEvaluatedKey: {}".format(
            request_num, consolidated_response['Count'], ddb_response.get('LastEvaluatedKey')))
        if consolidated_response['Count'] >= requested_results:
            break
    # end while
    if fetch_more_than_one:
        if 'LastEvaluatedKey' in ddb_response:
            consolidated_response['LastEvaluatedKey'] = ddb_response.get('LastEvaluatedKey')
        else:
            video_time = time.strptime(video_date, '%Y-%m-%d')
            prev_day = time.localtime(time.mktime(video_time) - 86400)
            video_date = time.strftime('%Y-%m-%d', prev_day)
            consolidated_response['LastEvaluatedKey'] = {
                'capture_date': video_date
            }

    consolidated_response = generate_signed_uri(consolidated_response)
    return {
        "statusCode": 200,
        "headers": cors_headers,
        "body": json.dumps(consolidated_response, cls=DecimalEncoder)
    }


def query_videos(dyndb, camera_name, video_date, filter_expression, num_results, older_than_ts, newer_than_ts):
    """
    Build and execute the DynamoDB query and return the raw response.
    - If camera_name is provided (and no explicit date), query per-camera table.
    - Else, query the daily timeline table by capture_date.
    """
    # When requesting by camera timeline, ignore video_date so we get most recent by camera.
    by_camera = camera_name is not None and video_date is None

    if by_camera:
        table = dyndb.Table('security_alarm_videos')
        key_cond = Key('camera_name').eq(camera_name)

        # Range direction and condition
        scan_forward = False
        if older_than_ts is not None:
            key_cond = key_cond & Key('event_ts').lt(older_than_ts)
            scan_forward = False
        elif newer_than_ts is not None:
            key_cond = key_cond & Key('event_ts').gt(newer_than_ts)
            scan_forward = True

        return table.query(
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=key_cond,
            ScanIndexForward=scan_forward,
            Limit=num_results,
        )

    # Default: timeline by date (capture_date)
    table = dyndb.Table('security_video_timeline')
    # If no date was supplied at all (and no camera), default should already be set by the handler.
    if not video_date:
        video_date = time.strftime('%Y-%m-%d')

    key_cond = Key('capture_date').eq(video_date)
    scan_forward = False
    if older_than_ts is not None:
        key_cond = key_cond & Key('event_ts').lt(older_than_ts)
        scan_forward = False
    elif newer_than_ts is not None:
        key_cond = key_cond & Key('event_ts').gt(newer_than_ts)
        scan_forward = True
        
    if filter_expression is not None:
        num_results = 100 # Increase limit when filtering, as filtering happens after limit. 

        return table.query(
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=key_cond,
            FilterExpression=filter_expression,
            ScanIndexForward=scan_forward,
            Limit=num_results,
        )
    else:
        return table.query(
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=key_cond,
            ScanIndexForward=scan_forward,
            Limit=num_results,
        )


def generate_signed_uri(data):
    """ Generates signed URIs for the videos - allowing app to load them.

    :param data: List of videos for which signed URIs will be generated.
    :return: The updated data with signed URIs
    """

    s3_client = boto3.client('s3')
    bucket = "security-alarms"
    new_items = []

    for item in data['Items']:
        url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket,
                'Key': item['object_key']
            }
        )
        item['uri'] = url
        if 'object_key_small' in item:
            url = s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket,
                    'Key': item['object_key_small']
                }
            )
            item['uri_small_video'] = url
        # fin
        if 'thumbnail_key' in item and item['thumbnail_key'] != "":
            url = s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket,
                    'Key': item['thumbnail_key']
                }
            )
            item['thumbnail_uri'] = url
        new_items.append(item)

    # end for

    data['Items'] = new_items

    return data

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