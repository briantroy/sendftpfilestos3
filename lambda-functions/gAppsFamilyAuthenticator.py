""" Evaluates a user's google token to determine if they have access to the
    APIs
"""
from __future__ import print_function


def lambda_handler(event, context):
    """ Handles the lambda event and evaluates the validity of the user's access.

    :param event: Lambda Event
    :param context: Lambda Context
    :return:
    """
    import urllib2
    import json

    allowed_domain = "brianandkelly.ws"

    print(event)
    # Get the user info from Google for the recieved token...
    id_token = event['authorizationToken']
    google_token_helper_uri = "https://www.googleapis.com/oauth2/v3/tokeninfo?id_token=" + id_token

    result = json.loads(urllib2.urlopen(google_token_helper_uri).read())

    domain = result['hd']
    user = result['sub']
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
                    "Resource": "arn:aws:execute-api:us-east-1:*:!!your api info!!*"
                }
            ]
        }
    }

    return respond
