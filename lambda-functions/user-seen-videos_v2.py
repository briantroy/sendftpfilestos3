"""
Lambda function to retrieve user viewed events and videos from DynamoDB tables.

This is a READ-ONLY function that queries the event-views and video-views tables.
Views are saved in real-time via the save-event-video Lambda function.

QUERY PARAMETERS:
=================
- since (optional): ISO 8601 timestamp to retrieve only items viewed after this time
  Examples: ?since=2025-01-27T14:35:22.000Z or ?since=2025-01-27T14:35:22Z

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
- MAX_EVENTS_LIMIT: Maximum number of event views to return (default: 500)
- MAX_VIDEOS_LIMIT: Maximum number of video views to return (default: 500)
"""

import json
import boto3
import os
from botocore.exceptions import ClientError
from datetime import datetime

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

# Limits for number of items to return
MAX_EVENTS_LIMIT = int(os.environ.get('MAX_EVENTS_LIMIT', '500'))
MAX_VIDEOS_LIMIT = int(os.environ.get('MAX_VIDEOS_LIMIT', '500'))

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


def validate_timestamp(timestamp_str):
    """
    Validate that a timestamp string is in valid ISO 8601 format.
    Returns the validated timestamp string if valid, None otherwise.

    Accepts formats like:
    - 2025-01-27T14:35:22.000Z
    - 2025-01-27T14:35:22Z
    - 2025-01-27T14:35:22.123456Z
    - 2025-01-27T14:35:22
    """
    if not timestamp_str or not isinstance(timestamp_str, str):
        return None

    # Try to parse the timestamp using common ISO 8601 formats
    timestamp_formats = [
        '%Y-%m-%dT%H:%M:%S.%fZ',  # With milliseconds and Z
        '%Y-%m-%dT%H:%M:%SZ',      # Without milliseconds, with Z
        '%Y-%m-%dT%H:%M:%S.%f',    # With milliseconds, no Z
        '%Y-%m-%dT%H:%M:%S',       # Without milliseconds or Z
    ]

    for fmt in timestamp_formats:
        try:
            datetime.strptime(timestamp_str, fmt)
            # If parsing succeeds, return the original string
            return timestamp_str
        except ValueError:
            continue

    # If none of the formats matched, return None
    return None


def handle_get(event):
    """
    Handle GET request to retrieve viewed videos for a user.
    Queries the event-views and video-views tables.
    Supports optional 'since' query parameter to filter by timestamp.
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

        # Get optional 'since' timestamp parameter
        since_timestamp = query_parameters.get('since')
        if since_timestamp:
            # Validate the timestamp format
            validated_since = validate_timestamp(since_timestamp)
            if validated_since is None:
                return create_response(400, {
                    'error': 'Invalid since parameter',
                    'message': 'The since parameter must be a valid ISO 8601 timestamp (e.g., 2025-01-27T14:35:22.000Z or 2025-01-27T14:35:22Z)'
                }, event)
            since_timestamp = validated_since

        # Get user events and videos from the two tables
        try:
            events_result = get_user_items(event_views_table, user_id, 'event_id', MAX_EVENTS_LIMIT, since_timestamp)
            videos_result = get_user_items(video_views_table, user_id, 'video_id', MAX_VIDEOS_LIMIT, since_timestamp)
        except ClientError as e:
            print(f"Error retrieving record for user {user_id}: {e}")
            return create_response(500, {
                'error': 'Failed to retrieve viewed videos',
                'message': str(e)
            }, event)

        viewed_events = events_result['items']
        viewed_videos = videos_result['items']
        latest_event_timestamp = events_result['latest_timestamp']
        latest_video_timestamp = videos_result['latest_timestamp']

        if not viewed_events and not viewed_videos:
            return create_response(404, {
                'error': 'User not found',
                'userId': user_id
            }, event)

        # Get the most recent timestamp from the actual items returned
        timestamp = None
        if latest_event_timestamp and latest_video_timestamp:
            timestamp = max(latest_event_timestamp, latest_video_timestamp)
        elif latest_event_timestamp:
            timestamp = latest_event_timestamp
        elif latest_video_timestamp:
            timestamp = latest_video_timestamp

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


def get_user_items(table, user_id, item_id_field, limit, since_timestamp=None):
    """
    Query a DynamoDB table to get items for a user up to the specified limit.
    Returns a dictionary containing the list of item IDs and the latest timestamp.
    Queries using viewed_timestamp as sort key for efficient time-ordered retrieval.

    Args:
        table: DynamoDB table resource
        user_id: User identifier
        item_id_field: Field name for the item ID ('event_id' or 'video_id')
        limit: Maximum number of items to return from DynamoDB
        since_timestamp: Optional timestamp to filter items newer than this time

    Returns:
        dict: {
            'items': list of item IDs (most recent first),
            'latest_timestamp': the most recent viewed_timestamp from the returned items (or None)
        }
    """
    try:
        # Build the query parameters
        if since_timestamp:
            # Query with both user_id and timestamp filter
            response = table.query(
                KeyConditionExpression='user_id = :uid AND viewed_timestamp > :since',
                ExpressionAttributeValues={
                    ':uid': user_id,
                    ':since': since_timestamp
                },
                ScanIndexForward=False,  # Sort descending (most recent first)
                Limit=limit
            )
            print(f"Querying {table.table_name} for user {user_id} since {since_timestamp}")
        else:
            # Query with just user_id
            response = table.query(
                KeyConditionExpression='user_id = :uid',
                ExpressionAttributeValues={
                    ':uid': user_id
                },
                ScanIndexForward=False,  # Sort descending (most recent first)
                Limit=limit
            )

        items = response.get('Items', [])

        # Extract the item IDs from the results
        # Use a set to track seen IDs and preserve order (most recent first)
        seen_ids = set()
        item_ids = []
        latest_timestamp = None

        for item in items:
            # Track the latest timestamp (first item has the most recent timestamp due to sort order)
            if latest_timestamp is None and 'viewed_timestamp' in item:
                latest_timestamp = item['viewed_timestamp']

            if item_id_field in item:
                item_id = item[item_id_field]
                # Only add if we haven't seen this ID yet (deduplication)
                if item_id not in seen_ids:
                    seen_ids.add(item_id)
                    item_ids.append(item_id)

        print(f"Retrieved {len(item_ids)} unique items from {table.table_name} for user {user_id} (limit: {limit})")

        return {
            'items': item_ids,
            'latest_timestamp': latest_timestamp
        }

    except ClientError as e:
        print(f"Error querying {table.table_name} for user {user_id}: {e}")
        raise e


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