import os
import json
import jwt
from datetime import datetime, timedelta
from google.oauth2 import id_token as google_id_token
from google.auth.transport import requests as google_requests


GOOGLE_CLIENT_ID = os.environ["GOOGLE_CLIENT_ID"]
JWT_SECRET = os.environ["JWT_SECRET"]
GOOGLE_ALLOWED_DOMAIN = os.environ.get("ALLOWED_DOMAIN", "brianandkelly.ws")

def lambda_handler(event, context):
    allowed_origins = [
        "https://security-videos.brianandkelly.ws",
        "https://sec-vid-dev.brianandkelly.ws",
        "http://localhost:3000"
    ]
    origin = event.get('headers', {}).get('origin')
    
    if origin in allowed_origins:
        print("Origin allowed: ", origin)
        cors_headers = {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Vary": "Origin"
        }
    else:
        print("Origin not allowed: ", origin)
        cors_headers = {
            "Access-Control-Allow-Origin": "https://security-videos.brianandkelly.ws",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Vary": "Origin"
        }
    # Step 1: Read id_token from POST

    print(cors_headers)
    try:
        data = json.loads(event["body"])
        id_token = data.get("id_token")
        if not id_token:
            print("fail: Missing id_token")
            return _http_response(400, {"error": "Missing id_token"})
    except Exception as e:
        print("fail: Invalid JSON")
        return _http_response(400, {"error": "Invalid JSON"})

    # Step 2: Verify Google id_token
    try:
        idinfo = google_id_token.verify_oauth2_token(id_token, google_requests.Request(), GOOGLE_CLIENT_ID)
        user_id = idinfo["sub"]
        email = idinfo["email"]
    except Exception as e:
        print("fail: invalid google token")
        return _http_response(401, {"error": "Invalid Google token"})


    # Step 3: Restrict to users in allowed domain using 'hd' claim
    hd_claim = idinfo.get("hd")
    if hd_claim != GOOGLE_ALLOWED_DOMAIN:
        return _http_response(403, {"error": f"Google account must be in domain {GOOGLE_ALLOWED_DOMAIN}"})

    # Step 4: Create your own JWT (session token)
    payload = {
        "user_id": user_id,
        "email": email,
        "exp": datetime.utcnow() + timedelta(days=10)
    }
    session_token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
    print("session_token: ", session_token)

    # Step 5: Return Set-Cookie header
    cookie = (
        f"session_token={session_token}; "
        f"Path=/; "
        f"HttpOnly; "
        f"Secure; "
        f"SameSite=None; "
        f"Max-Age={10*24*60*60}"
    )

    headers = {
        "Set-Cookie": cookie,
        "Content-Type": "application/json",
        **cors_headers
    }
    return {
        "statusCode": 200,
        "headers": headers,
        "body": json.dumps({"message": "Logged in"})
    }

def _http_response(status, body, cors_headers=None):
    print("In _http_response")
    headers = {
        "Content-Type": "application/json"
    }
    if cors_headers:
        headers.update(cors_headers)
    return {
        "statusCode": status,
        "headers": headers,
        "body": json.dumps(body)
    }