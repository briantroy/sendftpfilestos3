import json
import boto3
import os
from datetime import datetime
from botocore.exceptions import ClientError
import urllib

# Allowed origins for CORS
ALLOWED_ORIGINS = {
    "https://security-videos.brianandkelly.ws",
    "https://sec-vid-dev.brianandkelly.ws",
    "http://localhost:3000"
}

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
table_name = os.environ.get('DYNAMODB_TABLE_NAME', 'user-viewed-videos')
table = dynamodb.Table(table_name)

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
        
        # Get existing record to merge with
        try:
            response = table.get_item(Key={'user_id': user_id})
            existing_item = response.get('Item')
        except ClientError as e:
            print(f"Error checking existing record: {e}")
            existing_item = None
        
        # Initialize variables for merging
        merged_events = []
        merged_videos = []
        created_at = current_time
        
        if existing_item:
            # Parse existing data
            try:
                existing_events = json.loads(existing_item.get('viewed_events', '[]'))
                existing_videos = json.loads(existing_item.get('viewed_videos', '[]'))
                created_at = existing_item.get('created_at', current_time)
                
                print(f"Existing events: {len(existing_events)}, new events: {len(new_viewed_events)}")
                print(f"Existing videos: {len(existing_videos)}, new videos: {len(new_viewed_videos)}")
                
            except json.JSONDecodeError as e:
                print(f"Error parsing existing data, starting fresh: {e}")
                existing_events = []
                existing_videos = []
        else:
            existing_events = []
            existing_videos = []
            print(f"No existing record found, creating new one")
        
        # Merge viewed events (preserve order, avoid duplicates)
        merged_events = existing_events.copy()
        new_events_added = 0
        for viewed_event in new_viewed_events:
            if viewed_event not in merged_events:
                merged_events.append(viewed_event)
                new_events_added += 1
        
        # Merge viewed videos (preserve order, avoid duplicates)
        merged_videos = existing_videos.copy()
        new_videos_added = 0
        for video in new_viewed_videos:
            if video not in merged_videos:
                merged_videos.append(video)
                new_videos_added += 1
        
        print(f"Merged {new_events_added} new events, {new_videos_added} new videos")
        
        # Convert merged arrays to JSON strings for storage
        merged_events_json = json.dumps(merged_events)
        merged_videos_json = json.dumps(merged_videos)
        
        # Save merged data
        try:
            table.put_item(
                Item={
                    'user_id': user_id,
                    'viewed_events': merged_events_json,
                    'viewed_videos': merged_videos_json,
                    'event_timestamp': event_timestamp,
                    'created_at': created_at,
                    'updated_at': current_time
                }
            )
            
            status_code = 200 if existing_item else 201
            message = 'Viewed videos merged successfully' if existing_item else 'Viewed videos saved successfully'
            
            # Determine if this was a merge operation (data changed)
            was_merged = existing_item and (new_events_added > 0 or new_videos_added > 0)
            
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
            if not existing_item:
                response_data['createdAt'] = created_at
            
            # If this was a merge operation, include the complete merged dataset
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
        # Get user record from DynamoDB
        try:
            response = table.get_item(Key={'user_id': user_id})
            item = response.get('Item')
        except ClientError as e:
            print(f"Error retrieving record for user {user_id}: {e}")
            return create_response(500, {
                'error': 'Failed to retrieve viewed videos',
                'message': str(e)
            }, event)
        
        if not item:
            return create_response(404, {
                'error': 'User not found',
                'userId': user_id
            }, event)
        
        # Parse the JSON strings back to lists
        try:
            viewed_events = json.loads(item.get('viewed_events', '[]'))
            viewed_videos = json.loads(item.get('viewed_videos', '[]'))
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON data for user {user_id}: {e}")
            return create_response(500, {
                'error': 'Data corruption: invalid JSON in stored data',
                'message': str(e)
            }, event)
        
        return create_response(200, {
            'userId': user_id,
            'timestamp': item.get('event_timestamp'),
            'viewedEvents': viewed_events,
            'viewedVideos': viewed_videos,
            'viewedEventsCount': len(viewed_events),
            'viewedVideosCount': len(viewed_videos),
            'createdAt': item.get('created_at'),
            'updatedAt': item.get('updated_at')
        }, event)
    
    except Exception as e:
        print(f"Unexpected error in GET: {e}")
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
    """
    try:
        # Prepare batch get request
        request_items = {
            table_name: {
                'Keys': [{'user_id': user_id} for user_id in user_ids]
            }
        }
        
        response = dynamodb.batch_get_item(RequestItems=request_items)
        items = response.get('Responses', {}).get(table_name, [])
        
        # Process results
        results = {}
        for item in items:
            user_id = item['user_id']
            try:
                viewed_events = json.loads(item.get('viewed_events', '[]'))
                viewed_videos = json.loads(item.get('viewed_videos', '[]'))
                
                results[user_id] = {
                    'userId': user_id,
                    'timestamp': item.get('event_timestamp'),
                    'viewedEvents': viewed_events,
                    'viewedVideos': viewed_videos,
                    'viewedEventsCount': len(viewed_events),
                    'viewedVideosCount': len(viewed_videos),
                    'createdAt': item.get('created_at'),
                    'updatedAt': item.get('updated_at')
                }
            except json.JSONDecodeError as e:
                print(f"Error parsing data for user {user_id}: {e}")
                results[user_id] = {'error': 'Data corruption'}
        
        return results
        
    except ClientError as e:
        print(f"Error in batch get: {e}")
        raise e