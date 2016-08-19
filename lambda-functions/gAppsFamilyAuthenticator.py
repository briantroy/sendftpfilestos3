from __future__ import print_function

def lambda_handler(event, context):
    import urllib2
    import json

    allowed_domain = "brianandkelly.ws"

    print(event)
    # Get the user info from Google for the recieved token...
    id_token = event['authorizationToken']
    requestARN = event['methodArn']
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
                                            "Resource": "arn:aws:execute-api:us-east-1:*:7k8o0sgjli/securityvideos/*"
                                          }
                                        ]
                      }
                    }

    return respond