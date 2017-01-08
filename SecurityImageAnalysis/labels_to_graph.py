import boto3
import botocore
from neo4j.v1 import GraphDatabase, basic_auth
import time
import datetime
import pytz
from decimal import Decimal
from boto3.dynamodb.conditions import Key
import json
import logging
import logging.handlers
import os
import sys


def main():

    dyndb = boto3.resource('dynamodb')
    vid_table = dyndb.Table(dynamodb_label_table)
    projection = get_config_item(app_config, "dynamo_projection")

    start_time = time.time()
    while True:
        checkpoint = fetch_checkpoint()
        if checkpoint is None:
            print "No checkpoint found in S3 - Loading entire set of Rekognition Labels in 20 seconds... " + \
                            "Kill now to abort..."
            app_logger.info("No checkpoint found in S3 - Loading entire set of Rekognition Labels in 20 seconds... "
                            "Kill now to abort...")
            time.sleep(1)
            for i in range(1, 20):
                print str(20-i)
                time.sleep(1)
            # End Wait For Loop
            print "Starting full load!!!"
            app_logger.info("Starting full load...")

            get_full_scan_data(vid_table, projection)
            checkpoint = get_scan_with_capture_date(vid_table, projection)

        # FIN Full Load Condition

        app_logger.info("Processing from checkpoint - date: {} timestamp: {}".format(checkpoint['max_capture_date'],
                                                                                 checkpoint['max_timestamp']))
        get_query_items_since_checkpoint(checkpoint, vid_table, projection)
        app_logger.info("Run complete. Uptime: {} seconds".format((time.time() - start_time)))
        # pause for 5 minutes
        time.sleep(300)
    # End While


def get_full_scan_data(vid_table, projection):
    batch_number = 1
    start_time = time.time()
    app_logger.info("Starting full scan load.")
    response = vid_table.scan(
            ProjectionExpression=projection,
            FilterExpression='attribute_not_exists(capture_date)',
            Limit=items_per_batch,
    )
    app_logger.info("Processing batch {} in full scan load...".format(str(batch_number)))
    process_response_items(response)
    while 'LastEvaluatedKey' in response:
        batch_number += 1
        response = vid_table.scan(
            ProjectionExpression=projection,
            ExclusiveStartKey=response['LastEvaluatedKey'],
            Limit=items_per_batch
        )
        app_logger.info("Processing batch {} in full scan load...".format(str(batch_number)))
        process_response_items(response)

    # End While

    app_logger.info("Full Scan Load complete in {} seconds.".format(time.time() - start_time))


def get_scan_with_capture_date(vid_table, projection):
    s3 = boto3.resource('s3')
    index_to_use = 'capture_date-event_ts-index'
    batch_number = 1
    start_time = time.time()
    app_logger.info("Starting capture date index full scan.")
    response = vid_table.scan(
        IndexName=index_to_use,
        ProjectionExpression=projection,
        Limit=items_per_batch,
    )
    app_logger.info("Processing batch {} in capture date scan load...".format(str(batch_number)))
    max_values = process_response_items(response)
    while 'LastEvaluatedKey' in response:
        batch_number += 1
        response = vid_table.scan(
            IndexName=index_to_use,
            ProjectionExpression=projection,
            ExclusiveStartKey=response['LastEvaluatedKey'],
            Limit=items_per_batch
        )
        app_logger.info("Processing batch {} in capture date scan load...".format(str(batch_number)))
        max_values = process_response_items(response, max_values)

        s3.Object('security-alarms', 'status/label_to_graph_checkpoint').put(Body=json.dumps(max_values))

    app_logger.info("Capture Date Index Scan Load complete in {} seconds.".format(time.time() - start_time))
    return max_values


def get_query_items_since_checkpoint(checkpoint, vid_table, projection):
    s3 = boto3.resource('s3')
    index_to_use = 'capture_date-event_ts-index'
    batch_number = 1
    start_time = time.time()
    response = vid_table.query(
        IndexName=index_to_use,
        ProjectionExpression=projection,
        KeyConditionExpression=Key('capture_date').eq(checkpoint['max_capture_date']) &
                               Key('event_ts').gt(Decimal(checkpoint['max_timestamp'])),
        Limit=items_per_batch
    )
    app_logger.info("Processing batch {} in checkpoint load...".format(str(batch_number)))
    max_values = process_response_items(response, checkpoint)
    # check to see if we have 0 images and we've crossed a day boundary
    if len(response['Items']) == 0:
        # check date vs. current date
        ts_date = datetime.datetime.fromtimestamp(
            int(max_values['max_timestamp'])
        ).strftime('%Y-%m-%d')
        now_date = datetime.datetime.now().strftime('%Y-%m-%d')

        app_logger.info("Timestamp Date: {} - Current Date: {}".format(ts_date, now_date))
        if ts_date != now_date:
            app_logger.info("0 Records found and Timestamp Date: {} - does not equal the Current "
                            "Date: {} updating checkpoint to current date.".format(ts_date, now_date))
            max_values['max_capture_date'] = now_date
        # FIN
    # FIN
    s3.Object('security-alarms', 'status/label_to_graph_checkpoint').put(Body=json.dumps(max_values))
    while 'LastEvaluatedKey' in response:
        batch_number += 1
        response = vid_table.query(
            IndexName=index_to_use,
            ProjectionExpression=projection,
            KeyConditionExpression=Key('capture_date').eq(checkpoint['max_capture_date']) &
                                   Key('event_ts').gt(Decimal(checkpoint['max_timestamp'])),
            ExclusiveStartKey=response['LastEvaluatedKey'],
            Limit=items_per_batch
        )
        app_logger.info("Processing batch {} in checkpoint load...".format(str(batch_number)))
        max_values = process_response_items(response, checkpoint)
        s3.Object('security-alarms', 'status/label_to_graph_checkpoint').put(Body=json.dumps(max_values))
    # End While
    app_logger.info("Checkpoint Load complete in {} seconds.".format(time.time() - start_time))


def fetch_checkpoint():
    s3 = boto3.resource('s3')

    try:
        response = s3.Object(s3_bucket, checkpoint_s3_object_name).get()
        checkpoint_file = response['Body'].read()
        return json.loads(checkpoint_file)
    except botocore.exceptions.ClientError as e:
        app_logger.info("Unable to fetch checkpoint object from S3.")
        app_logger.info("Object: {}/{}".format(s3_bucket, checkpoint_s3_object_name))
        app_logger.info("Error Message: {}".format(e))


def process_response_items(response, max_values={}):
    start_time = time.time()
    max_timestamp = "0"
    max_capture_date = "NOT SET"
    if 'max_timestamp' in max_values:
        max_timestamp = max_values['max_timestamp']
    if 'max_capture_date' in max_values:
        max_capture_date = max_values['max_capture_date']

    app_logger.info("Now processing {} items in last response.".format(str(len(response['Items']))))

    for item in response['Items']:
            event_ts = "0"
            if 'event_ts' in item:
                event_ts = item['event_ts']
                if Decimal(str(item['event_ts'])) > Decimal(max_timestamp):
                    max_timestamp = str(item['event_ts'])
                    if 'capture_date' in item:
                        max_capture_date = item['capture_date']
                    # FIN
                # FIN

            process_row_to_graph(item['object_key'], item['label'], item['confidence'], event_ts)
            # print line_out

    app_logger.info("Completed processing {} items in {} seconds.".format(str(len(response['Items'])), (time.time() -
                                                                                                        start_time)))
    return {'max_timestamp': max_timestamp, 'max_capture_date': max_capture_date}


def process_row_to_graph(object_key, label_name, confidence, event_ts=0):
    camera_name = parse_camera_name_from_object_key(object_key)

    if camera_name != 'garage' and camera_name != 'crawlspace':

        date_info = parse_date_time_from_object_key(object_key)

        add_camera_node = 'MERGE(this_camera:Camera {camera_name: "' + camera_name + '"})'
        add_image_node = 'MERGE(this_image:Image {object_key: "' + object_key + \
                         '", isodate: "' + date_info['isodate'] + \
                         '", timestamp: ' + str(event_ts) + '})'
        add_label_node = 'MERGE(this_label:Label {label_name: "' + label_name + '"})'
        add_isodate_node = 'MERGE(this_isodate:ISODate {iso_date: "' + date_info['isodate'] + '"})'
        add_year_node = 'MERGE(this_year:Year {year_value: ' + date_info['year'] + '})'
        add_month_node = 'MERGE(this_month:Month {month_value: ' + date_info['month'] + '})'
        add_day_node = 'MERGE(this_day:Day {day_value: ' + date_info['day'] + '})'
        add_hour_node = 'MERGE(this_hour:Hour {hour_value: ' + date_info['hour'] + '})'
        relate_image_to_label = 'MERGE (this_image)-[:HAS_LABEL {confidence: ' + str(confidence) + '}]->(this_label)'
        relate_image_to_camera = 'MERGE (this_camera)-[:HAS_IMAGE {timestamp: ' + str(event_ts) + '}]->(this_image)'
        relate_image_to_timestamp = 'MERGE (this_image)-[:HAS_TIMESTAMP]->(this_isodate)'
        relate_image_to_year = 'MERGE (this_image)-[:HAS_YEAR]->(this_year)'
        relate_image_to_month = 'MERGE (this_image)-[:HAS_MONTH]->(this_month)'
        relate_image_to_day = 'MERGE (this_image)-[:HAS_DAY]->(this_day)'
        relate_image_to_hour = 'MERGE (this_image)-[:HAS_HOUR]->(this_hour)'

        full_query_list = add_camera_node + "\n" + \
            add_image_node + "\n" + \
            add_label_node + " " + \
            add_isodate_node + " " + \
            add_year_node + " " + \
            add_month_node + " " + \
            add_day_node + " " + \
            add_hour_node + " " + \
            relate_image_to_label + " " + \
            relate_image_to_camera + " " + \
            relate_image_to_timestamp + " " + \
            relate_image_to_year + " " + \
            relate_image_to_month + " " + \
            relate_image_to_day + " " + \
            relate_image_to_hour

        neo_session = driver.session()

        tx = neo_session.begin_transaction()

        tx.run(full_query_list)

        # END FOR

        tx.commit()
        neo_session.close()
        return True

        # print("Object: " + object_key + " written.")
    # FIN


def parse_camera_name_from_object_key(object_key):
    first_parts = object_key.split("/")
    return first_parts[1]


def parse_date_time_from_object_key(object_key):
    pacific = pytz.timezone('America/Los_Angeles')

    first_parts = object_key.split("/")
    last_part_idx = len(first_parts) - 1
    file_name = first_parts[last_part_idx]

    # now parse the date and time out of the file name
    second_parts = file_name.split("_")
    last_part_idx = len(second_parts) - 1
    date_time_string = second_parts[last_part_idx]
    if date_time_string.endswith('.jpg'):
        date_time_string = date_time_string[:-4]

    final_parts = date_time_string.split("-")
    date_part = final_parts[0]
    time_part = final_parts[1]

    # parse out our date
    year = date_part[:4]
    date_part = date_part[4:]
    month = date_part[:2]
    day = date_part[2:]

    # parse out the time
    hour = time_part[:2]
    time_part = time_part[2:]
    seconds = time_part[2:]
    minutes = time_part[:2]

    if hour[:1] == '0':
        hour = hour[1:]
    if month[:1] == '0':
        month = month[1:]
    if day[:1] == '0':
        day = day[1:]

    this_date = datetime.datetime(int(year), int(month), int(day), int(hour),
                                  int(minutes), int(seconds), 0, pacific)

    return_dict = {'isodate': this_date.isoformat(),
                   'year': year,
                   'month': month,
                   'day': day,
                   'hour': hour}

    return return_dict


def logger_setup(app_config):
    """ Sets up the application logger at startup

    :param app_config: Dict containing the application config.
    :return: The logging handler
    """
    # set up logger
    app_log_file = get_config_item(app_config, 'app_log_file.file')

    app_logger = logging.getLogger('AppLogger')
    app_logger.setLevel(logging.DEBUG)

    try:
        # Add the log message handler to the logger
        handler = logging.handlers.RotatingFileHandler(
            app_log_file, maxBytes=get_config_item(app_config, 'app_log_file.rotate_at_in_bytes'),
            backupCount=4)
        formatter = logging.Formatter(get_config_item(app_config, 'app_log_file.log_format'))
        handler.setFormatter(formatter)

        app_logger.addHandler(handler)
    except IOError:
        print "Can not open the log file: {}... exiting...".format(app_log_file)
        return False
    # end try

    return app_logger
# end logger_setup


def config_reader(config_file):
    """ Reads and validates the config file specified.

    :param config_file: The config file (default or passed in on command line)
    :return: configuration dict - or false if an error occurs.
    """
    if os.path.exists(config_file):
        with open(config_file, 'r') as cfile:
            app_config = json.load(cfile)
        # end with
        return app_config
    else:
        print "The config file: {} does not exist, please try again.".format(config_file)
        return False
    # fin

# end config_reader


def get_config_item(app_config, item):
    """ Gets a specified parameter from the configuration. Nested parameters
     are provided to this function with dot notation e.g., foo.bar.baz

    :param app_config: Configuration dict
    :param item: Dot notation for parameter to return.
    :return:
    """
    item_path = item.split('.')
    this_config = app_config
    for path_part in item_path:
        this_config = this_config[path_part]
    # end For

    return this_config
# end get_config_item


def check_config_file():
    """ validates either the default or command line provided config file.

    :return: Configuration Dict
    """
    # Locate and init config.
    default_config = "config.json"
    if len(sys.argv) == 2:
        # config from command line
        app_config = config_reader(sys.argv[1])
    else:
        # config should be in default
        app_config = config_reader(default_config)
    # fin
    if not app_config:
        print "Exiting due to invalid config file."
        return False
    # fin
    return app_config
# end check_config_file

# Main code - setup here...
# Locate and init config.
app_config = check_config_file()
if not app_config:
    sys.exit()

# set up logger
app_logger = logger_setup(app_config)
if not app_logger:
    sys.exit()

# Graph Server Connection Info
graph_server_host = get_config_item(app_config, "neo4j.host")
graph_server_user = get_config_item(app_config, "neo4j.username")
graph_server_pwd = get_config_item(app_config, "neo4j.password")
driver = GraphDatabase.driver("bolt://" + graph_server_host, auth=basic_auth(graph_server_user, graph_server_pwd))

dynamodb_label_table = get_config_item(app_config, "dynamodb_source_table")

s3_bucket = get_config_item(app_config, "s3_checkpoint_info.bucket_name")
checkpoint_s3_object_name = get_config_item(app_config, "s3_checkpoint_info.object_name")

items_per_batch = get_config_item(app_config, "items_per_batch")


if __name__ == "__main__":
    main()