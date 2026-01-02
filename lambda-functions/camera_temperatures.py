import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
from decimal import Decimal
import os

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get('DYNAMODB_TABLE_NAME', 'camera_temperature_history')
table = dynamodb.Table(table_name)

ALLOWED_ORIGINS = {
    "https://security-videos.brianandkelly.ws",
    "https://sec-vid-dev.brianandkelly.ws",
    "http://localhost:3000"
}


class DecimalEncoder(json.JSONEncoder):
    """Helper class to convert DynamoDB Decimal types to JSON-serializable types"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


def get_latest_for_all_cameras(cors_headers):
    """
    Get the most recent temperature reading for each camera.
    Returns a dict with camera names as keys and their latest readings.
    """
    try:
        # Scan to get all unique camera names
        response = table.scan(
            ProjectionExpression='camera_name'
        )
        
        # Get unique camera names
        camera_names = set()
        for item in response.get('Items', []):
            camera_names.add(item['camera_name'])
        
        # Handle pagination if necessary
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ProjectionExpression='camera_name',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for item in response.get('Items', []):
                camera_names.add(item['camera_name'])
        
        # Get latest reading for each camera
        results = {}
        for camera_name in camera_names:
            response = table.query(
                KeyConditionExpression=Key('camera_name').eq(camera_name),
                ScanIndexForward=False,  # Sort descending (newest first)
                Limit=1
            )
            
            if response.get('Items'):
                item = response['Items'][0]
                results[camera_name] = {
                    'temperature': item.get('temperature'),
                    'timestamp': item.get('temperature_ts'),
                    'unit': item.get('unit', 'F')  # Default to Fahrenheit if not specified
                }
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'cameras': results,
                'count': len(results)
            }, cls=DecimalEncoder)
        }

    except Exception as e:
        print(f"Error getting latest readings: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'error': 'Failed to retrieve temperature readings',
                'message': str(e)
            })
        }


def get_camera_history(camera_name, hours, cors_headers):
    """
    Get temperature history for a specific camera over the specified number of hours.

    Args:
        camera_name: Name of the camera
        hours: Number of hours of history to retrieve
        cors_headers: CORS headers to include in response
    """
    try:
        # Calculate the timestamp threshold (Unix timestamp)
        threshold_time = datetime.utcnow() - timedelta(hours=hours)
        threshold_timestamp = int(threshold_time.timestamp())

        response = table.query(
            KeyConditionExpression=Key('camera_name').eq(camera_name) &
                                 Key('temperature_ts').gte(threshold_timestamp),
            ScanIndexForward=True  # Sort ascending (oldest first for graphing)
        )

        readings = response.get('Items', [])

        # Handle pagination if there are many readings
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('camera_name').eq(camera_name) &
                                     Key('temperature_ts').gte(threshold_timestamp),
                ScanIndexForward=True,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            readings.extend(response.get('Items', []))

        # Format the response for graphing
        formatted_readings = [
            {
                'timestamp': item.get('temperature_ts'),
                'temperature': item.get('temperature'),
                'unit': item.get('unit', 'F')
            }
            for item in readings
        ]
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'camera_name': camera_name,
                'hours': hours,
                'count': len(formatted_readings),
                'readings': formatted_readings
            }, cls=DecimalEncoder)
        }

    except Exception as e:
        print(f"Error getting camera history: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'error': 'Failed to retrieve camera history',
                'message': str(e)
            })
        }


def lambda_handler(event, context):
    """
    Main Lambda handler function.
    Routes requests based on the path and HTTP method.

    API Endpoints:
        GET /temperatures/latest - Get latest reading for all cameras
        GET /temperatures/history?camera=<name>&hours=<hours> - Get history for a camera
    """

    print(f"Event: {json.dumps(event)}")

    # Get the Origin header (handle both possible capitalizations)
    origin = event.get('headers', {}).get('origin') or event.get('headers', {}).get('Origin')

    cors_headers = {
        'Access-Control-Allow-Credentials': 'true',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Access-Control-Allow-Methods': 'GET,OPTIONS',
        'Vary': 'Origin'
    }

    # Only echo allowed origins
    if origin in ALLOWED_ORIGINS:
        print("Origin allowed: " + str(origin))
        cors_headers['Access-Control-Allow-Origin'] = origin
    else:
        print("Origin not allowed: " + str(origin))

    # Extract path and query parameters
    path = event.get('path', '')
    query_params = event.get('queryStringParameters') or {}
    http_method = event.get('httpMethod', 'GET')

    # Handle preflight OPTIONS request
    if http_method == 'OPTIONS':
        return {
            'statusCode': 204,
            'headers': cors_headers,
            'body': ''
        }

    # Validate HTTP method
    if http_method != 'GET':
        return {
            'statusCode': 405,
            'headers': cors_headers,
            'body': json.dumps({
                'error': 'Method not allowed',
                'message': 'Only GET requests are supported'
            })
        }
    
    # Route to appropriate handler
    if path.endswith('/latest'):
        return get_latest_for_all_cameras(cors_headers)

    elif path.endswith('/history'):
        camera_name = query_params.get('camera')

        if not camera_name:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({
                    'error': 'Missing required parameter',
                    'message': 'camera parameter is required'
                })
            }

        # Get hours parameter, default to 24
        try:
            hours = int(query_params.get('hours', 24))
            if hours <= 0:
                raise ValueError("Hours must be positive")
        except ValueError as e:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({
                    'error': 'Invalid parameter',
                    'message': f'hours must be a positive integer: {str(e)}'
                })
            }

        return get_camera_history(camera_name, hours, cors_headers)

    else:
        return {
            'statusCode': 404,
            'headers': cors_headers,
            'body': json.dumps({
                'error': 'Not found',
                'message': 'Valid endpoints: /temperatures/latest, /temperatures/history'
            })
        }