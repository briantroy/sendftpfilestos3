"""
Lambda function to save individual event or video view records to DynamoDB.

DYNAMODB TABLE SCHEMA:
=====================

Table: event-views
------------------
Partition Key: user_id (String)
Sort Key: viewed_timestamp (String) - ISO 8601 timestamp when the event was viewed

Attributes:
- user_id: The user identifier
- event_id: The event identifier
- event_timestamp: ISO 8601 timestamp when the event occurred
- viewed_timestamp: ISO 8601 timestamp when the event was viewed
- created_at: ISO 8601 timestamp when the record was created

GSI: event-timestamp-index
- Partition Key: event_id (String)
- Sort Key: event_timestamp (String)

Table: video-views
------------------
Partition Key: user_id (String)
Sort Key: viewed_timestamp (String) - ISO 8601 timestamp when the video was viewed

Attributes:
- user_id: The user identifier
- video_id: The video identifier
- video_timestamp: ISO 8601 timestamp when the video occurred
- viewed_timestamp: ISO 8601 timestamp when the video was viewed
- created_at: ISO 8601 timestamp when the record was created

GSI: video-timestamp-index
- Partition Key: video_id (String)
- Sort Key: video_timestamp (String)

ENVIRONMENT VARIABLES:
======================
- AWS_REGION: AWS region (default: us-east-1)
- EVENT_VIEWS_TABLE_NAME: DynamoDB table name for event views (default: event-views)
- VIDEO_VIEWS_TABLE_NAME: DynamoDB table name for video views (default: video-views)
"""

import json
import boto3
import os
from datetime import datetime
from botocore.exceptions import ClientError

# Allowed origins for CORS
ALLOWED_ORIGINS = {
    "https://security-videos.brianandkelly.ws",
    "https://sec-vid-dev.brianandkelly.ws",
    "http://localhost:3000"
}

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
event_views_table_name = os.environ.get('EVENT_VIEWS_TABLE_NAME', 'event-views')
video_views_table_name = os.environ.get('VIDEO_VIEWS_TABLE_NAME', 'video-views')
event_views_table = dynamodb.Table(event_views_table_name)
video_views_table = dynamodb.Table(video_views_table_name)

def lambda_handler(event, context):
    """
    Main Lambda handler that routes requests based on HTTP method
    """
    http_method = event.get('httpMethod', '').upper()

    print(f"Received {http_method} request")

    # Handle OPTIONS request for CORS preflight
    if http_method == 'OPTIONS':
        return handle_options(event)

    # Handle POST/PUT request to save event or video view
    elif http_method in ['POST', 'PUT']:
        return handle_save(event)

    else:
        return create_response(405, {
            'error': 'Method not allowed',
            'allowedMethods': ['OPTIONS', 'POST', 'PUT']
        }, event)


def get_cors_origin(event):
    """
    Get the appropriate CORS origin based on the request origin
    """
    # Check if event is a dictionary and has headers
    if not isinstance(event, dict) or not event.get('headers'):
        print("Event is not a dictionary or has no headers, using default: *")
        print(event)
        return '*'

    request_origin = event.get('headers', {}).get('Origin') or event.get('headers', {}).get('origin')

    if request_origin and request_origin in ALLOWED_ORIGINS:
        print(f"Request origin allowed: {request_origin}")
        return request_origin

    # Default to * if no origin or origin not in allowed list
    print(f"Request origin not allowed or not present, using default: *")
    return '*'


def handle_options(event):
    """
    Handle OPTIONS request for CORS preflight
    """
    cors_origin = get_cors_origin(event)

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': cors_origin,
            'Access-Control-Allow-Methods': 'POST, PUT, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
            'Access-Control-Max-Age': '86400',
            'Access-Control-Allow-Credentials': 'true' if cors_origin != '*' else 'false'
        },
        'body': ''
    }


def handle_save(event):
    """
    Handle POST/PUT request to save an event or video view record
    Determines which table to use based on the payload (eventId vs videoId)
    """
    try:
        # Get userId from path parameters or query parameters
        path_parameters = event.get('pathParameters') or {}
        query_parameters = event.get('queryStringParameters') or {}

        # Try different sources for userId
        user_id = (
            path_parameters.get('userId') or
            path_parameters.get('user_id') or
            query_parameters.get('userId') or
            query_parameters.get('user_id')
        )

        if not user_id:
            return create_response(400, {
                'error': 'userId is required in path parameters',
                'hint': 'Provide userId in path (/save-view/{userId}) or query parameters (?userId=xxx)'
            }, event)

        # Parse the request body
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event.get('body', {})

        # Determine if this is an event or video based on the payload
        event_id = body.get('eventId')
        video_id = body.get('videoId')

        # Validate that we have either eventId or videoId (but not both)
        if event_id and video_id:
            return create_response(400, {
                'error': 'Cannot specify both eventId and videoId. Provide only one.'
            }, event)

        if not event_id and not video_id:
            return create_response(400, {
                'error': 'Either eventId or videoId is required'
            }, event)

        # Get the timestamps
        item_timestamp = body.get('timestamp')  # When the event/video occurred
        viewed_timestamp = body.get('viewedTimestamp')  # When it was viewed

        # Validate required fields
        if not item_timestamp:
            return create_response(400, {
                'error': 'timestamp is required (when the event/video occurred)'
            }, event)

        if not viewed_timestamp:
            return create_response(400, {
                'error': 'viewedTimestamp is required (when the event/video was viewed)'
            }, event)

        current_time = datetime.utcnow().isoformat()

        # Save to the appropriate table
        try:
            if event_id:
                # Save to event-views table
                item = {
                    'user_id': user_id,
                    'viewed_timestamp': viewed_timestamp,
                    'event_id': event_id,
                    'event_timestamp': item_timestamp,
                    'created_at': current_time
                }
                event_views_table.put_item(Item=item)

                response_data = {
                    'message': 'Event view saved successfully',
                    'userId': user_id,
                    'eventId': event_id,
                    'eventTimestamp': item_timestamp,
                    'viewedTimestamp': viewed_timestamp,
                    'createdAt': current_time
                }
                print(f"Saved event view for user {user_id}: {event_id}")

            else:  # video_id
                # Save to video-views table
                item = {
                    'user_id': user_id,
                    'viewed_timestamp': viewed_timestamp,
                    'video_id': video_id,
                    'video_timestamp': item_timestamp,
                    'created_at': current_time
                }
                video_views_table.put_item(Item=item)

                response_data = {
                    'message': 'Video view saved successfully',
                    'userId': user_id,
                    'videoId': video_id,
                    'videoTimestamp': item_timestamp,
                    'viewedTimestamp': viewed_timestamp,
                    'createdAt': current_time
                }
                print(f"Saved video view for user {user_id}: {video_id}")

            return create_response(201, response_data, event)

        except ClientError as e:
            print(f"Error saving record: {e}")
            return create_response(500, {
                'error': 'Failed to save view record',
                'message': str(e)
            }, event)

    except json.JSONDecodeError as e:
        return create_response(400, {
            'error': 'Invalid JSON in request body',
            'message': str(e)
        }, event)

    except Exception as e:
        print(f"Unexpected error in save: {e}")
        return create_response(500, {
            'error': 'Internal server error',
            'message': str(e)
        }, event)


def create_response(status_code, body, event=None):
    """
    Helper function to create standardized HTTP responses with dynamic CORS headers
    """
    cors_origin = get_cors_origin(event) if event else '*'

    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': cors_origin,
            'Access-Control-Allow-Methods': 'POST, PUT, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
            'Access-Control-Allow-Credentials': 'true' if cors_origin != '*' else 'false'
        },
        'body': json.dumps(body, default=str)
    }
