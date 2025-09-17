import json

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

    cors_headers['Content-Type'] = 'application/json'
    # Expire the session_token cookie
    cors_headers['Set-Cookie'] = 'session_token=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/; HttpOnly; SameSite=None; Secure'
    cors_headers['Content-Type'] = 'application/json'
        # # Set the session_token cookie to expire immediately
    
    return {
        'statusCode': 200,
        'headers': cors_headers,
        'body': '{"message":"Logged out successfully"}'
    }