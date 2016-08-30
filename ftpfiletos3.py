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


def read_log_file(logger, app_config):
    """ Function reads the log file specified in the configuration for new lines
     containing the appropriate trigger string.

    :param logger: The logging handler to use.
    :param app_config: The configuration for the application.
    :return:
    """

    ftp_log_file = get_config_item(app_config, 'log_file_to_follow.file')
    while not os.path.exists(ftp_log_file):
        logger.info("VSFTPD log file doesn't exist yet... waiting...")
        time.sleep(1)
    # end while
    filesize = os.path.getsize(ftp_log_file)
    while filesize <= 64:
        logger.info("VSFTPD log file is less than 64 bytes... waiting...")
        time.sleep(1)
        filesize = os.path.getsize(ftp_log_file)
        print filesize
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
                threading.Thread(name=thread_name, target=parse_upload_file_line,
                                 args=(line, logger, app_config, )).start()
                line_count += 1
                if line_count % 10 == 0:
                    logger.info("THREAD-STATUS: There are {} currently active threads."
                                .format(threading.activeCount()))
                # fin
            # fin

    except KeyboardInterrupt:
        pass
# end read_log_file


def parse_upload_file_line(line, logger, app_config):
    """ Function parses the log file line for the information required to push
     the file to s3.

    :param line: The line found containing the trigger string
    :param logger: The logging handler
    :param app_config: The application configuration
    :return:
    """
    import datetime

    # Set Up
    base_dir = get_config_item(app_config, 'ftp_base_dir')

    start_timing = time.time()

    start_date = datetime.datetime.now()
    date_string = start_date.strftime('%Y') + "-" + start_date.strftime("%m") + "-" + \
                  start_date.strftime("%d")
    hour_string = "Hour-" + str(start_date.hour)

    line_parts = line.split(",")
    file_name = line_parts[1].strip()
    file_name = file_name.replace('"', '')
    logger.info("File for upload is: {} with file size: {}".format(file_name, line_parts[2]))
    if line_parts[2].find('Kbyte/sec') != -1:
        logger.info("Skippking file {} because it is empty.".format(file_name))
        sys.exit(0)
    # fin

    # Parse the file name to get the sub-folder and object name.
    path_end = file_name.replace(base_dir, "")
    path_parts = path_end.split('/')
    if len(path_parts) != 5:
        lastpart = len(path_parts) - 1
        # Clean up parens in the file name
        just_file = path_parts[lastpart].replace('(', '')
        just_file = just_file.replace(')', '')
        img_type = "snap"
    else:
        img_type = path_parts[3]
        just_file = path_parts[4]
    # fin

    if just_file.find('.mkv') != -1:
        # Convert mkv to mp4 file
        result = transcodetomp4(file_name)
        if result != file_name:
            file_name = result
            just_file = just_file.replace('.mkv', '.mp4')
        else:
            logger.error("File {} could not be transcoded to mp4.".format(file_name))
            sys.exit(0)
        # fin
    # fin
    camera_name = path_parts[1]

    push_file_to_s3(logger, app_config, camera_name, date_string, hour_string, img_type, just_file,
                    file_name, start_timing)
    sys.exit(0)
# end parse_upload_file_line


def push_file_to_s3(logger, app_config, camera, date_part, hour_part, img_type,
                    s3_file_name, local_file, start_timing):
    """ Fuction uploads the specified file to s3

    :param logger: The application logging handler
    :param app_config: The application config
    :param camera: The name of the camera
    :param date_part: The date part of the s3 object path/name
    :param hour_part: The hour part of the s3 object path/name
    :param img_type: The type of image - video or still generally
    :param s3_file_name: The object/file name for s3 with no prefix
    :param local_file: The full path to the local file
    :param start_timing: When processing of the log file line started - used to output
                         the total processing time.
    :return:
    """
    import boto3
    s3_resource = boto3.resource('s3')
    logging.getLogger('boto3').addHandler(logger)
    s3_object = get_config_item(app_config, 's3_info.object_base') + \
                '/' + camera + '/' + date_part + '/' + hour_part + '/' + \
                img_type + '/' + s3_file_name
    s3_resource.Object(get_config_item(app_config, 's3_info.bucket_name'), s3_object).\
        put(Body=open(local_file, 'rb'))
    totaltime = time.time() - start_timing
    logger.info("S3 Object: {} written to s3 in {} seconds.".format(s3_object, totaltime))
    sys.exit(0)
# end push_file_to_s3


def transcodetomp4(file_in):
    """ Transcodes our .mkv file to .mp4 prior to upload to s3

    :param file_in: The full path to the .mkv file.
    :return: The full path to the resulting .mp4 file
    """

    import subprocess

    file_out = file_in.replace('.mkv', '.mp4')

    convert_command = '/usr/bin/avconv -i "{}" -f mp4 -vcodec copy -acodec " +' \
                      'libfaac -b:a 112k -ac 2 -y "{}"' \
                      .format(file_in, file_out)

    try:
        subprocess.check_call(convert_command, shell=True)
    except subprocess.CalledProcessError:
        return file_in

    return file_out
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

if __name__ == "__main__":
    main()
