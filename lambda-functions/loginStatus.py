import jwt
import os
import json
from urllib.parse import unquote

JWT_SECRET = os.environ['JWT_SECRET']
JWT_ALG = "HS256"

def get_cookie_value(cookie_header, cookie_name):
    if not cookie_header:
        print("No cookie header found")
        return None
    cookies = cookie_header.split(';')
    print("Cookies: ", cookies)
    for cookie in cookies:
        name, _, value = cookie.strip().partition('=')
        if name == cookie_name:
            print(f"Found cookie: {name}={value}")  
            return value
    return None



ALLOWED_ORIGINS = {
    "https://security-videos.brianandkelly.ws",
    "https://sec-vid-dev.brianandkelly.ws",
    "http://localhost:3000"
}

def lambda_handler(event, context):
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
        cors_headers['Access-Control-Allow-Origin'] = origin

    # Figure out HTTP method (works with both HTTP API and REST API event shapes)
    method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')

    # Handle preflight OPTIONS request
    if method == 'OPTIONS':
        return {
            'statusCode': 204,
            'headers': cors_headers,
            'body': ''
        }


    # 1. Get cookies from headers
    headers = event['headers'] or {}
    cookie_string = headers.get('cookie') or headers.get('Cookie') # case-insensitive
    print
    token = None
    if cookie_string:
        token = get_cookie_value(cookie_string, 'session_token')
        if token:
            print("Found session_token in cookie")
            token = unquote(token)
        else:
            print("session_token cookie not found") 
            token = unquote(cookie_string)
    
    principal_id = "anonymous"
    response = {
        'email': '',
        'picture': '',
        'username': '',
        'user_id': principal_id
    }

    if token:
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
            print("Good Token, JWT payload:", payload)
            principal_id = payload['user_id']
            response = { 
                'email': payload.get('email', ''),
                'picture': payload.get('picture', ''),
                'username': payload.get('username', ''),
                'user_id': principal_id
            }
            effect = "Allow"
            policy_context = {'user_id': principal_id}
        except jwt.ExpiredSignatureError:
            print
            pass # expired, effect remains Deny
        except Exception as e:
            print("Invalid token; could not decode: ", e)
            pass
    
    return {
        'statusCode': 200,
        'headers': cors_headers,
        'body': json.dumps(response)
    }