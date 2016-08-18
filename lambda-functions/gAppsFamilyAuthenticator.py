from __future__ import print_function

def lambda_handler(event, context):
    import urllib2
    import json

    api_id_security_videos = "xxxxxxxx"
    personal_user_id = "xxxxxxxxx"
    allowed_domain = "example.com"

    print(event)
    # Get the user info from Google for the recieved token...
    id_token = event['authorizationToken']
    google_token_helper_uri = "https://www.googleapis.com/oauth2/v3/tokeninfo?id_token=" + id_token

    result = json.loads(urllib2.urlopen(google_token_helper_uri).read())

    domain = result['hd']
    user = result['sub']
    email = result['email']
    effect = 'Deny'

    if domain == allowed_domain:
        effect = 'Allow'

    respond = {
                    "principalId": user,
                    "policyDocument": {
                                        "Version": "2012-10-17",
                                        "Statement": [
                                          {
                                            "Action": "execute-api:Invoke",
                                            "Effect": effect,
                                            "Resource": "arn:aws:execute-api:us-east-1:" + personal_user_id + ":" + api_id_security_videos + "/*/GET/"
                                          }
                                        ]
                      }
                    }

    return respond