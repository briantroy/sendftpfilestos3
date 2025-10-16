"""
Lambda function to track user viewed events and videos using two separate DynamoDB tables.

DYNAMODB TABLE SCHEMA:
=====================

Table: user-seen-events
-----------------------
Partition Key: user_id (String)
Sort Key: timestamp_event_id (String) - Composite key format: "2025-01-15T10:30:00.000Z#a1b2c3d4e5f6g7h8"
  (timestamp + # + first 16 chars of SHA256 hash of event_id)

Attributes:
- user_id: The user identifier
- timestamp_event_id: Composite sort key (timestamp#hash) for time-ordered queries
- event_id: The clean event ID (e.g., "event-123" or long S3 key)
- timestamp: ISO 8601 timestamp when the event was viewed
- created_at: ISO 8601 timestamp when the record was created

Table: user-seen-videos
-----------------------
Partition Key: user_id (String)
Sort Key: timestamp_video_id (String) - Composite key format: "2025-01-15T10:30:00.000Z#a1b2c3d4e5f6g7h8"
  (timestamp + # + first 16 chars of SHA256 hash of video_id)

Attributes:
- user_id: The user identifier
- timestamp_video_id: Composite sort key (timestamp#hash) for time-ordered queries
- video_id: The clean video ID (e.g., "video-456" or long S3 key)
- timestamp: ISO 8601 timestamp when the video was viewed
- created_at: ISO 8601 timestamp when the record was created

WHY COMPOSITE SORT KEYS WITH HASHING?
======================================
1. Time-ordered queries: Get most recent items efficiently with ScanIndexForward=False
2. Uniqueness: Hash of ID + timestamp prevents duplicate entries at the same time
3. Size limit compliance: Hash keeps sort key under DynamoDB's 1024-byte limit
4. Scalability: Avoids 400KB item size limit by storing each view as a separate item
5. Efficient pagination: Can limit queries to most recent N items without missing data
6. Clean IDs available: Full event_id and video_id stored separately for easy access

ENVIRONMENT VARIABLES:
======================
- AWS_REGION: AWS region (default: us-east-1)
- EVENTS_TABLE_NAME: DynamoDB table name for events (default: user-seen-events)
- VIDEOS_TABLE_NAME: DynamoDB table name for videos (default: user-seen-videos)
"""

import json
import boto3
import os
from datetime import datetime
from botocore.exceptions import ClientError
import urllib
import hashlib

# Allowed origins for CORS
ALLOWED_ORIGINS = {
    "https://security-videos.brianandkelly.ws",
    "https://sec-vid-dev.brianandkelly.ws",
    "http://localhost:3000"
}

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
events_table_name = os.environ.get('EVENTS_TABLE_NAME', 'user-seen-events')
videos_table_name = os.environ.get('VIDEOS_TABLE_NAME', 'user-seen-videos')
events_table = dynamodb.Table(events_table_name)
videos_table = dynamodb.Table(videos_table_name)

def lambda_handler(event, context):
    """
    Main Lambda handler that routes requests based on HTTP method
    """
    http_method = event.get('httpMethod', '').upper()
    
    print(f"Received {http_method} request")
    
    # Handle OPTIONS request for CORS preflight
    if http_method == 'OPTIONS':
        return handle_options(event)
    
    # Handle POST request to save/update viewed videos
    elif http_method == 'POST':
        return handle_post(event)
    
    # Handle GET request to retrieve viewed videos
    elif http_method == 'GET':
        return handle_get(event)
    
    else:
        return create_response(405, {
            'error': 'Method not allowed',
            'allowedMethods': ['OPTIONS', 'POST', 'GET']
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
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
            'Access-Control-Max-Age': '86400',
            'Access-Control-Allow-Credentials': 'true' if cors_origin != '*' else 'false'
        },
        'body': ''
    }


def handle_post(event):
    """
    Handle POST request to merge and save user viewed videos
    Merges new viewed videos with existing ones to support multi-device usage
    Uses two separate DynamoDB tables for events and videos
    """
    try:
        # Parse the request body
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event.get('body', {})

        user_id = body.get('userId')
        timestamp = body.get('timestamp')
        new_viewed_events = body.get('viewedEvents', [])
        new_viewed_videos = body.get('viewedVideos', [])

        # Validate required fields
        if not user_id:
            return create_response(400, {'error': 'userId is required'}, event)

        if not isinstance(new_viewed_events, list):
            return create_response(400, {'error': 'viewedEvents must be a valid array'}, event)

        if not isinstance(new_viewed_videos, list):
            return create_response(400, {'error': 'viewedVideos must be a valid array'}, event)

        current_time = datetime.utcnow().isoformat()
        event_timestamp = timestamp if timestamp else current_time

        # Get existing events and videos from the two tables
        existing_events = get_user_items(events_table, user_id, 'event_id')
        existing_videos = get_user_items(videos_table, user_id, 'video_id')

        print(f"Existing events: {len(existing_events)}, new events: {len(new_viewed_events)}")
        print(f"Existing videos: {len(existing_videos)}, new videos: {len(new_viewed_videos)}")

        # Determine if user had any data before (for status code and response)
        had_existing_data = len(existing_events) > 0 or len(existing_videos) > 0

        # Convert to sets for efficient lookup
        existing_events_set = set(existing_events)
        existing_videos_set = set(existing_videos)

        # Identify which items from the client are actually new
        new_events_to_save = [e for e in new_viewed_events if e not in existing_events_set]
        new_videos_to_save = [v for v in new_viewed_videos if v not in existing_videos_set]

        new_events_added = len(new_events_to_save)
        new_videos_added = len(new_videos_to_save)

        print(f"Found {new_events_added} new events, {new_videos_added} new videos to save")

        # Save only the new items to DynamoDB
        try:
            # Write new events
            if new_events_added > 0:
                batch_write_items(events_table, user_id, new_events_to_save, 'event_id', event_timestamp)
                print(f"Saved {len(new_events_to_save)} new events")

            # Write new videos
            if new_videos_added > 0:
                batch_write_items(videos_table, user_id, new_videos_to_save, 'video_id', event_timestamp)
                print(f"Saved {len(new_videos_to_save)} new videos")

            # After saving, get the complete merged list from DynamoDB
            # This ensures we return data sorted by timestamp (most recent first)
            # This is critical for multi-device sync - the client gets the complete, correctly ordered list
            if new_events_added > 0 or new_videos_added > 0:
                merged_events = get_user_items(events_table, user_id, 'event_id')
                merged_videos = get_user_items(videos_table, user_id, 'video_id')
            else:
                # No new data was added, use what we already fetched
                merged_events = existing_events
                merged_videos = existing_videos

            status_code = 200 if had_existing_data else 201
            message = 'Viewed videos merged successfully' if had_existing_data else 'Viewed videos saved successfully'

            # Determine if this was a merge operation (data changed) or if client sent empty lists
            client_sent_empty_lists = len(new_viewed_events) == 0 and len(new_viewed_videos) == 0
            client_sent_data = len(new_viewed_events) > 0 or len(new_viewed_videos) > 0
            was_merged = had_existing_data and (client_sent_data or client_sent_empty_lists)

            response_data = {
                'message': message,
                'userId': user_id,
                'mergedEventsCount': len(merged_events),
                'mergedVideosCount': len(merged_videos),
                'newEventsAdded': new_events_added,
                'newVideosAdded': new_videos_added,
                'existingEventsCount': len(existing_events),
                'existingVideosCount': len(existing_videos),
                'eventTimestamp': event_timestamp,
                'updatedAt': current_time,
                'wasMerged': was_merged
            }

            # Add createdAt only for new records
            if not had_existing_data:
                response_data['createdAt'] = current_time

            # If this was a merge operation or client sent empty lists, include the complete merged dataset
            if was_merged:
                response_data['viewedEvents'] = merged_events
                response_data['viewedVideos'] = merged_videos
                response_data['timestamp'] = event_timestamp

            return create_response(status_code, response_data, event)

        except ClientError as e:
            print(f"Error saving merged record: {e}")
            return create_response(500, {
                'error': 'Failed to save merged viewed videos',
                'message': str(e)
            }, event)

    except json.JSONDecodeError as e:
        return create_response(400, {
            'error': 'Invalid JSON in request body',
            'message': str(e)
        }, event)

    except Exception as e:
        print(f"Unexpected error in POST: {e}")
        return create_response(500, {
            'error': 'Internal server error',
            'message': str(e)
        }, event)


def handle_get(event):
    """
    Handle GET request to retrieve viewed videos for a user
    Queries the two separate DynamoDB tables
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
        user_id = urllib.parse.unquote(user_id)

        # Get user events and videos from the two tables
        try:
            viewed_events = get_user_items(events_table, user_id, 'event_id')
            viewed_videos = get_user_items(videos_table, user_id, 'video_id')
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
        timestamp = get_latest_timestamp(events_table, videos_table, user_id)

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
    Query a DynamoDB table to get all items for a user
    Returns a list of item IDs (in chronological order, most recent first)
    Queries using composite sort key for efficient time-ordered retrieval
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

        # Extract the clean item IDs from the results (not the composite key)
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


def batch_write_items(table, user_id, item_ids, item_id_field, timestamp):
    """
    Batch write items to a DynamoDB table
    Handles batches of up to 25 items (DynamoDB limit)
    Uses composite sort key: timestamp#hash for time-ordered queries with uniqueness
    The hash ensures we stay under the 1024-byte sort key limit for long IDs
    """
    if not item_ids:
        return

    current_time = datetime.utcnow().isoformat()

    # Determine the composite sort key field name based on item type
    if item_id_field == 'event_id':
        composite_key_field = 'timestamp_event_id'
    else:  # video_id
        composite_key_field = 'timestamp_video_id'

    # Process in batches of 25 (DynamoDB batch_write_item limit)
    batch_size = 25
    for i in range(0, len(item_ids), batch_size):
        batch = item_ids[i:i + batch_size]

        with table.batch_writer() as writer:
            for item_id in batch:
                # Create a hash of the item_id to keep sort key under 1024 bytes
                # Using first 16 chars of SHA256 hash provides good uniqueness while staying compact
                id_hash = hashlib.sha256(item_id.encode()).hexdigest()[:16]

                # Create composite sort key: timestamp#hash
                # Format: "2025-01-15T10:30:00.000Z#a1b2c3d4e5f6g7h8"
                composite_sort_key = f"{timestamp}#{id_hash}"

                writer.put_item(
                    Item={
                        'user_id': user_id,
                        composite_key_field: composite_sort_key,
                        item_id_field: item_id,  # Store clean ID as separate attribute
                        'timestamp': timestamp,
                        'created_at': current_time
                    }
                )

        print(f"Wrote batch of {len(batch)} items to {table.table_name}")


def get_latest_timestamp(events_table, videos_table, user_id):
    """
    Get the most recent timestamp from both tables for a user
    Uses composite sort key to efficiently get the latest item
    """
    latest_timestamp = None

    try:
        # Query both tables for the most recent item (composite sort key is already time-sorted)
        for table in [events_table, videos_table]:
            response = table.query(
                KeyConditionExpression='user_id = :uid',
                ExpressionAttributeValues={
                    ':uid': user_id
                },
                ScanIndexForward=False,  # Sort descending (most recent first)
                Limit=1
            )

            items = response.get('Items', [])
            if items and 'timestamp' in items[0]:
                timestamp = items[0]['timestamp']
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
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
            'Access-Control-Allow-Credentials': 'true' if cors_origin != '*' else 'false'
        },
        'body': json.dumps(body, default=str)
    }


# Additional utility function for batch operations (optional)
def handle_batch_get(user_ids):
    """
    Utility function to get viewed videos for multiple users at once
    Queries the two separate DynamoDB tables
    """
    try:
        results = {}

        for user_id in user_ids:
            try:
                viewed_events = get_user_items(events_table, user_id, 'event_id')
                viewed_videos = get_user_items(videos_table, user_id, 'video_id')
                timestamp = get_latest_timestamp(events_table, videos_table, user_id)

                results[user_id] = {
                    'userId': user_id,
                    'timestamp': timestamp,
                    'viewedEvents': viewed_events,
                    'viewedVideos': viewed_videos,
                    'viewedEventsCount': len(viewed_events),
                    'viewedVideosCount': len(viewed_videos)
                }
            except ClientError as e:
                print(f"Error getting data for user {user_id}: {e}")
                results[user_id] = {'error': str(e)}

        return results

    except Exception as e:
        print(f"Error in batch get: {e}")
        raise e