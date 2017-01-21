""" Module reads a specified log file and uploads files found in that log file to s3.
    Designed for use with vsftpd logs.
"""
import threading
import os
import sys
import time
import json
import logging
import logging.handlers
import pytz
import datetime
import calendar
from tail import follow


def main():
    """ Main function - runs the s3 uploader

    Keyword arguments:
    Takes one command line argument (optionally) - the configuration json file.
    Defaults to config.json in the current directory (note: not the install dir).


    :return:
    """
    # Locate and init config.
    app_config = check_config_file()
    if not app_config:
        sys.exit()

    # set up logger
    app_logger = logger_setup(app_config)
    if not app_logger:
        sys.exit()

    # PID file
    if not create_pid_file(app_config, app_logger):
        sys.exit()

    app_logger.info("STARTUP: Starting now - getting VSFTPD log file...")

    threading.Thread(name='log-reader', target=read_log_file,
                     args=(app_logger, app_config, )).start()

# end Main


def create_pid_file(app_config, app_logger):
    """ Creates the pid file.

    :param app_config: Dict containing the app config
    :param app_logger: The logging handler
    :return:
    """
    pid = str(os.getpid())
    pidfile = get_config_item(app_config, 'app_pid_file')

    if os.path.isfile(pidfile):
        print "{} already exists, exiting".format(pidfile)
        app_logger.info("STARTUP: PID file exists... exiting...")
        return False
    try:
        with (open(pidfile, 'w')) as pidfilestream:
            pidfilestream.write(pid)
            pidfilestream.close()
            return True
        # end with
    except IOError:
        app_logger.error("STARTUP: Could not create pid file at: {}".format(pidfile))
        return False

# end create_pid_file


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


def read_log_file(logger, app_config, is_test=False):
    """ Function reads the log file specified in the configuration for new lines
     containing the appropriate trigger string.

    :param logger: The logging handler to use.
    :param app_config: The configuration for the application.
    :param is_test: Default False - set to true to avoid calling line parser function while testing.
    :return:
    """

    ftp_log_file = get_config_item(app_config, 'log_file_to_follow.file')
    while not os.path.exists(ftp_log_file):
        logger.info("VSFTPD log file doesn't exist yet... waiting...")
        time.sleep(1)
        if is_test:
            return True
    # end while
    filesize = os.path.getsize(ftp_log_file)
    while filesize <= 64:
        logger.info("VSFTPD log file is less than 64 bytes... waiting...")
        time.sleep(1)
        filesize = os.path.getsize(ftp_log_file)
        if is_test:
            return True
    # end while

    logger.info("STARTUP: Beginning trace of VSFTPD log file.")
    fstream = open(ftp_log_file, "rt")
    fstream.seek(-64, 2)
    line_count = 1
    try:
        line_trigger = get_config_item(app_config, 'log_file_to_follow.line_identifier')
        for line in follow(fstream):
            if line_trigger in line:
                thread_name = 'line-handler-' + str(line_count)
                if not is_test:
                    threading.Thread(name=thread_name, target=parse_upload_file_line,
                                     args=(line, logger, app_config, )).start()
                line_count += 1
                if line_count % 10 == 0:
                    logger.info("THREAD-STATUS: There are {} currently active threads."
                                .format(threading.activeCount()))
                # fin
                if is_test:
                    return True
            # fin

    except KeyboardInterrupt:
        pass
# end read_log_file


def parse_upload_file_line(line, logger, app_config, is_test=False):
    """ Function parses the log file line for the information required to push
     the file to s3.

    :param line: The line found containing the trigger string
    :param logger: The logging handler
    :param app_config: The application configuration
    :param is_test: Default False - set true to test the function and get a valid return.
    :return:
    """
    import datetime

    s3_object_info = {}
    # Set Up
    base_dir = get_config_item(app_config, 'ftp_base_dir')

    start_timing = time.time()

    start_date = datetime.datetime.now()
    s3_object_info['date_string'] = start_date.strftime('%Y') + "-" + \
                                    start_date.strftime("%m") + "-" + \
                                    start_date.strftime("%d")
    s3_object_info['hour_string'] = "Hour-" + str(start_date.hour)

    line_parts = line.split(",")
    s3_object_info['file_name'] = line_parts[1].strip()
    s3_object_info['file_name'] = s3_object_info['file_name'].replace('"', '')
    logger.info("File for upload is: {} with file size: {}".
                format(s3_object_info['file_name'], line_parts[2]))
    if line_parts[2].find('Kbyte/sec') != -1:
        logger.info("Skippking file {} because it is empty.".format(s3_object_info['file_name']))
        if not is_test:
            sys.exit(0)
        return True
    # fin

    # Parse the file name to get the sub-folder and object name.
    path_end = s3_object_info['file_name'].replace(base_dir, "")
    path_parts = path_end.split('/')
    if len(path_parts) != 5:
        lastpart = len(path_parts) - 1
        # Clean up parens in the file name
        s3_object_info['just_file'] = path_parts[lastpart].replace('(', '')
        s3_object_info['just_file'] = s3_object_info['just_file'].replace(')', '')
        s3_object_info['img_type'] = "snap"
    else:
        s3_object_info['img_type'] = path_parts[3]
        s3_object_info['just_file'] = path_parts[4]
    # fin

    if s3_object_info['just_file'].find('.mkv') != -1:
        # Convert mkv to mp4 file
        result = transcodetomp4(s3_object_info['file_name'], logger)
        if result != s3_object_info['file_name']:
            s3_object_info['file_name'] = result
            s3_object_info['just_file'] = s3_object_info['just_file'].replace('.mkv', '.mp4')
        else:
            logger.error("File {} could not be transcoded to mp4.".
                         format(s3_object_info['file_name']))
            if not is_test:
                sys.exit(0)
            return True
        # fin
    # fin
    s3_object_info['camera_name'] = path_parts[1]

    if not is_test:
        s3_object_info['utc_ts'] = push_file_to_s3(logger, app_config, s3_object_info, start_timing)
        put_file_info_on_sqs(s3_object_info, logger, app_config)
        sys.exit(0)
    if not is_test:
        sys.exit(0)

    return True
# end parse_upload_file_line


def put_file_info_on_sqs(object_info, logger, app_config):
    # Get the service resource
    import boto3
    import json
    sqs = boto3.resource('sqs')

    # Get the queue
    queue = sqs.get_queue_by_name(QueueName='image_for_person_detection')
    logger.info("Putting message: {} on queue.".format(json.dumps(object_info)))
    response = queue.send_message(MessageBody=json.dumps(object_info))


def push_file_to_s3(logger, app_config, s3_object_info, start_timing):
    """ Fuction uploads the specified file to s3

    :param logger: The application logging handler
    :param app_config: The application config
    :param s3_object_info: Dict containing the information needed to upload the file to s3
    :param start_timing: When processing of the log file line started - used to output
                         the total processing time.
    :return:
    """
    import boto3
    s3_resource = boto3.resource('s3')
    logging.getLogger('boto3').addHandler(logger)
    s3_object = get_config_item(app_config, 's3_info.object_base') + \
                                            '/' + s3_object_info['camera_name'] + '/' + \
                                            s3_object_info['date_string'] + '/' + \
                                            s3_object_info['hour_string'] + '/' + \
                                            s3_object_info['img_type'] + '/' + \
                                            s3_object_info['just_file']
    utc_ts = parse_date_time_from_object_key(s3_object, s3_object_info['camera_name'], s3_object_info['img_type'])

    # Sometimes the camera provides a date days in the future. Catch that and use the current timestamp
    # when it occurs... look for timestamps more than 24 hours in the future
    if int(utc_ts) > (time.time() + 60*60*24):
        logger.info("Camera Timestamp ({}) is too far in the future. Using current timestamp instead."
                    .format(str(utc_ts)))
        utc_ts = time.time()
    # Fin

    object_metadata = {'camera': s3_object_info['camera_name'],
                       'camera_timestamp': str(utc_ts)}
    s3_resource.Object(get_config_item(app_config, 's3_info.bucket_name'),
                       s3_object).put(Body=open(s3_object_info['file_name'], 'rb'),
                                      Metadata=object_metadata)
    totaltime = time.time() - start_timing
    logger.info("S3 Object: {} written to s3 in {} seconds.".format(s3_object, totaltime))
    return utc_ts
# end push_file_to_s3


def transcodetomp4(file_in, logger):
    """ Transcodes our .mkv file to .mp4 prior to upload to s3

    :param file_in: The full path to the .mkv file.
    :return: The full path to the resulting .mp4 file
    """

    import subprocess

    file_out = file_in.replace('.mkv', '.mp4')

    if os.path.isfile('/usr/bin/avconv'):

        convert_command = '/usr/bin/avconv -i "{}" -f mp4 -vcodec copy -acodec '.format(file_in) + \
                          'libfaac -b:a 112k -ac 2 -y "{}"'.format(file_out)

        try:
            subprocess.check_call(convert_command, shell=True)
        except subprocess.CalledProcessError:
            logger.error("The command to transcode: {} --- failed...".format(convert_command))
            return file_in

        return file_out
    else:
        return file_in
    # fin
# end transcodetomp4


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


def parse_date_time_from_object_key(object_key, camera_name, type):
    """
    Parses the time/date info from the file name and creates a UTC timestamp.
    :param object_key:
    :return:
    """

    if camera_name == 'garage' or camera_name == 'crawlspace':
        return int(time.time())

    first_parts = object_key.split("/")
    last_part_idx = len(first_parts) - 1
    file_name = first_parts[last_part_idx]

    # now parse the date and time out of the file name
    second_parts = file_name.split("_")
    last_part_idx = len(second_parts) - 1
    if type == 'snap':
        date_time_string = second_parts[last_part_idx]
        if date_time_string.endswith('.jpg'):
            date_time_string = date_time_string[:-4]
        # FIN
        final_parts = date_time_string.split("-")
        date_part = final_parts[0]
        time_part = final_parts[1]

        # FIN
    # FIN
    if type == 'record':
        time_part = second_parts[last_part_idx]
        date_part = second_parts[(last_part_idx - 1)]
        if time_part.endswith('.mp4'):
            time_part = time_part[:-4]
    # FIN


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

    return convert_naive_local_to_utc_timestamp(year, month, day, hour, minutes, seconds)


def convert_naive_local_to_utc_timestamp(year, month, day, hour, minutes, seconds):
    """
    Converts a local naive date & time to a UTC Timestamp.
    :param year:
    :param month:
    :param day:
    :param hour:
    :param minutes:
    :param seconds:
    :return:
    """
    pacific = pytz.timezone('America/Los_Angeles')
    this_date = datetime.datetime(int(year), int(month), int(day), int(hour),
                                  int(minutes), int(seconds))
    local_dt = pacific.localize(this_date, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    timestamp = calendar.timegm(utc_dt.utctimetuple())
    return timestamp


if __name__ == "__main__":
    main()
