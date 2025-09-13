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
    # Step 1: Read id_token from POST
    try:
        data = json.loads(event["body"])
        id_token = data.get("id_token")
        if not id_token:
            return _http_response(400, {"error": "Missing id_token"})
    except Exception as e:
        return _http_response(400, {"error": "Invalid JSON"})

    # Step 2: Verify Google id_token
    try:
        idinfo = google_id_token.verify_oauth2_token(id_token, google_requests.Request(), GOOGLE_CLIENT_ID)
        user_id = idinfo["sub"]
        email = idinfo["email"]
    except Exception as e:
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

    # Step 5: Return Set-Cookie header
    cookie = (
        f"session_token={session_token}; "
        f"Path=/; "
        f"HttpOnly; "
        f"Secure; "
        f"SameSite=Lax; "
        f"Max-Age={10*24*60*60}"
    )

    return {
        "statusCode": 200,
        "headers": {
            "Set-Cookie": cookie,
            "Content-Type": "application/json",
            # Add CORS if your frontend is on a different domain:
            "Access-Control-Allow-Origin": "https://your-frontend.com",
            "Access-Control-Allow-Credentials": "true"
        },
        "body": json.dumps({"message": "Logged in"})
    }

def _http_response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            # And cors, if relevant:
            "Access-Control-Allow-Origin": "https://your-frontend.com",
            "Access-Control-Allow-Credentials": "true"
        },
        "body": json.dumps(body)
    }