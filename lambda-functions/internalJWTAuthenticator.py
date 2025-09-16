import os
import jwt
from urllib.parse import unquote

JWT_SECRET = os.environ['JWT_SECRET']
JWT_ALG = "HS256"

def get_cookie_value(cookie_header, cookie_name):
    if not cookie_header:
        return None
    cookies = cookie_header.split(';')
    for cookie in cookies:
        name, _, value = cookie.strip().partition('=')
        if name == cookie_name:
            return value
    return None

def lambda_handler(event, context):
    # 1. Get cookies from headers
    headers = event['headers'] or {}
    cookie_string = headers.get('cookie') or headers.get('Cookie') # case-insensitive
    token = None
    if cookie_string:
        # print(f"Got Cookie: {cookie_string}")
        token = get_cookie_value(cookie_string, 'session_token')
        if token:
            print("Found session_token in cookie")
            token = unquote(token)
        else:
            ## the cookie only contains the token
            token = cookie_string
            token = unquote(token)
    else: 
        print("No cookie found in headers")
    
    print(f"token: {token}")    
    print(f"cookie: {cookie_string}")
    principal_id = "anonymous"
    effect = "Deny"
    policy_context = {}

    if token:
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
            principal_id = payload['user_id']
            effect = "Allow"
            policy_context = {'user_id': principal_id}
        except jwt.ExpiredSignatureError:
            pass # expired, effect remains Deny
        except Exception:
            pass

    print(f"EFFECT: principal_id: {principal_id}, effect: {effect}")

    # Generate IAM policy for API Gateway
    auth_response = {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [{
                "Action": "execute-api:Invoke",
                "Effect": effect,
                "Resource": event["methodArn"]
            }]
        },
        "context": policy_context
    }
    return auth_response

# If using layers or zip deployment, ensure pyjwt is included!