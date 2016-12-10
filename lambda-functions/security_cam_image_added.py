""" Lambda function to handle adding new video files to s3 """
from __future__ import print_function

import urllib
import time
import boto3
from decimal import Decimal
import botocore_rekognition_beta


def lambda_handler(event, context):
    """ Lambda Handler """

    # print("Received event: " + json.dumps(event, indent=2))
    start_time = time.time()
    # Get the object from the event and show its content type
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).encode('utf8')
    size = event['Records'][0]['s3']['object']['size']
    print("Security Image: " + key + " with size: " + str(size) + " uploaded.")
    # print("Saving Data to DynamoDB")
    object_parts = key.split("/")
    camera_name = object_parts[1]
    # print("Camera Name: " + camera_name)

    dyndb = boto3.resource('dynamodb')
    img_table = dyndb.Table('security_alarm_images')
    img_timeline_table = dyndb.Table('security_image_timeline')
    save_data = {'camera_name': camera_name,
                 'image_size': size,
                 'image_name': object_parts[5],
                 'capture_date': object_parts[2],
                 'capture_hour': object_parts[3],
                 'event_ts': int(time.time()),
                 'object_key': key
                }

    img_table.put_item(Item=save_data)
    img_timeline_table.put_item(Item=save_data)

    get_rekognition_labels(key)

    print("Processing for " + key + " completed in: " + str(time.time() - start_time) +
          " seconds.")


def get_rekognition_labels(object_key):
    """
    Gets the object rekognition labels for the image.
    :param object_key:
    :return:
    """

    bucket = 'security-alarms'
    client = boto3.client('rekognition')

    request = {
        'Bucket': bucket,
        'Name': object_key
    }

    response = client.detect_labels(Image={'S3Object': request})

    write_labels_to_dynamo(object_key, response)


def write_labels_to_dynamo(object_key, labels):
    dyndb = boto3.resource('dynamodb')
    img_labels_table = dyndb.Table('security_alarm_image_label_set')

    for label_item in labels['Labels']:
        save_data = {
            'object_key': object_key,
            'label': label_item['Name'],
            'confidence': Decimal(str(label_item['Confidence']))
            }

        img_labels_table.put_item(Item=save_data)
    # end For