"""
Lambda function to retrieve user viewed events and videos from DynamoDB tables.

This is a READ-ONLY function that queries the event-views and video-views tables.
Views are saved in real-time via the save-event-video Lambda function.

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
    Main Lambda handler that routes requests based on HTTP method.
    This function only supports GET requests to retrieve viewed videos.
    Saving is handled by the save-event-video Lambda function.
    """
    http_method = event.get('httpMethod', '').upper()

    print(f"Received {http_method} request")

    # Handle OPTIONS request for CORS preflight
    if http_method == 'OPTIONS':
        return handle_options(event)

    # Handle GET request to retrieve viewed videos
    elif http_method == 'GET':
        return handle_get(event)

    else:
        return create_response(405, {
            'error': 'Method not allowed',
            'allowedMethods': ['OPTIONS', 'GET'],
            'message': 'This endpoint only supports GET requests. Use the save-event-video endpoint for saving views.'
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
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
            'Access-Control-Max-Age': '86400',
            'Access-Control-Allow-Credentials': 'true' if cors_origin != '*' else 'false'
        },
        'body': ''
    }


def handle_get(event):
    """
    Handle GET request to retrieve viewed videos for a user.
    Queries the event-views and video-views tables.
    """
    try:
        # Get userId from path parameters, query parameters, or request body
        path_parameters = event.get('pathParameters') or {}
        query_parameters = event.get('queryStringParameters') or {}

        # Try different sources for userId
        user_id = (
            path_parameters.get('userId') or
            path_parameters.get('user_id') or
            query_parameters.get('userId') or
            query_parameters.get('user_id')
        )

        # If no userId in parameters, try to get from body (for some API Gateway configurations)
        if not user_id and event.get('body'):
            try:
                if isinstance(event.get('body'), str):
                    body = json.loads(event['body'])
                else:
                    body = event.get('body', {})
                user_id = body.get('userId') or body.get('user_id')
            except json.JSONDecodeError:
                pass

        if not user_id:
            return create_response(400, {
                'error': 'userId is required',
                'hint': 'Provide userId in path parameters (/viewed-videos/{userId}), query parameters (?userId=xxx), or request body'
            }, event)

        # Note: Do NOT decode the user_id - DynamoDB stores it in the encoded format
        # user_id will be something like "brian.roy%40brianandkelly.ws"

        # Get user events and videos from the two tables
        try:
            viewed_events = get_user_items(event_views_table, user_id, 'event_id')
            viewed_videos = get_user_items(video_views_table, user_id, 'video_id')
        except ClientError as e:
            print(f"Error retrieving record for user {user_id}: {e}")
            return create_response(500, {
                'error': 'Failed to retrieve viewed videos',
                'message': str(e)
            }, event)

        if not viewed_events and not viewed_videos:
            return create_response(404, {
                'error': 'User not found',
                'userId': user_id
            }, event)

        # Get the most recent timestamp from both tables
        timestamp = get_latest_timestamp(event_views_table, video_views_table, user_id)

        return create_response(200, {
            'userId': user_id,
            'timestamp': timestamp,
            'viewedEvents': viewed_events,
            'viewedVideos': viewed_videos,
            'viewedEventsCount': len(viewed_events),
            'viewedVideosCount': len(viewed_videos)
        }, event)

    except Exception as e:
        print(f"Unexpected error in GET: {e}")
        return create_response(500, {
            'error': 'Internal server error',
            'message': str(e)
        }, event)


def get_user_items(table, user_id, item_id_field):
    """
    Query a DynamoDB table to get all items for a user.
    Returns a list of item IDs (in chronological order, most recent first).
    Queries using viewed_timestamp as sort key for efficient time-ordered retrieval.
    """
    try:
        response = table.query(
            KeyConditionExpression='user_id = :uid',
            ExpressionAttributeValues={
                ':uid': user_id
            },
            ScanIndexForward=False  # Sort descending (most recent first)
        )

        items = response.get('Items', [])

        # Handle pagination if there are more results
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression='user_id = :uid',
                ExpressionAttributeValues={
                    ':uid': user_id
                },
                ScanIndexForward=False,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))

        # Extract the item IDs from the results
        # Use a set to track seen IDs and preserve order (most recent first)
        seen_ids = set()
        item_ids = []

        for item in items:
            if item_id_field in item:
                item_id = item[item_id_field]
                # Only add if we haven't seen this ID yet (deduplication)
                if item_id not in seen_ids:
                    seen_ids.add(item_id)
                    item_ids.append(item_id)

        print(f"Retrieved {len(item_ids)} unique items from {table.table_name} for user {user_id}")
        return item_ids

    except ClientError as e:
        print(f"Error querying {table.table_name} for user {user_id}: {e}")
        raise e


def get_latest_timestamp(event_views_table, video_views_table, user_id):
    """
    Get the most recent viewed_timestamp from both tables for a user.
    Uses viewed_timestamp sort key to efficiently get the latest item.
    """
    latest_timestamp = None

    try:
        # Query both tables for the most recent item (viewed_timestamp is the sort key)
        for table in [event_views_table, video_views_table]:
            response = table.query(
                KeyConditionExpression='user_id = :uid',
                ExpressionAttributeValues={
                    ':uid': user_id
                },
                ScanIndexForward=False,  # Sort descending (most recent first)
                Limit=1
            )

            items = response.get('Items', [])
            if items and 'viewed_timestamp' in items[0]:
                timestamp = items[0]['viewed_timestamp']
                if not latest_timestamp or timestamp > latest_timestamp:
                    latest_timestamp = timestamp

    except ClientError as e:
        print(f"Error getting latest timestamp for user {user_id}: {e}")

    return latest_timestamp


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
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
            'Access-Control-Allow-Credentials': 'true' if cors_origin != '*' else 'false'
        },
        'body': json.dumps(body, default=str)
    }