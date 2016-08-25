import threading
from tail import follow
import logging
import logging.handlers
import os
import sys
import time
import json


def main():

    # Locate and init config.
    default_config = "config.json"
    if len(sys.argv) == 2:
        # config from command line
        app_config = config_reader(sys.argv[1])
    else:
        # config shoudl be in default
        app_config = config_reader(default_config)
    # fin
    if not app_config:
        print("Exiting due to invalid config file.")
        sys.exit()
    # fin

    pid = str(os.getpid())
    pidfile = get_config_item(app_config, 'app_pid_file')

    # set up logger
    app_log_file = get_config_item(app_config, 'app_log_file.file')

    app_logger = logging.getLogger('AppLogger')
    app_logger.setLevel(logging.DEBUG)

    # Add the log message handler to the logger
    handler = logging.handlers.RotatingFileHandler(
        app_log_file, maxBytes=get_config_item(app_config, 'app_log_file.rotate_at_in_bytes'), backupCount=4)
    formatter = logging.Formatter(get_config_item(app_config, 'app_log_file.log_format'))
    handler.setFormatter(formatter)

    app_logger.addHandler(handler)

    if os.path.isfile(pidfile):
        print("{} already exists, exiting".format(pidfile))
        app_logger.info("STARTUP: PID file exists... exiting...")
        sys.exit()
    with (open(pidfile, 'w')) as pidfilestream:
        pidfilestream.write(pid)
        pidfilestream.close()
    # end with

    app_logger.info("STARTUP: Starting now - getting VSFTPD log file...")

    t = threading.Thread(name='log-reader', target=read_log_file, args=(app_logger, app_config, )).start()

# end Main


def read_log_file(logger, app_config):

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
        print(filesize)
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
                t = threading.Thread(name=thread_name, target=parse_upload_file_line,
                                     args=(line, logger, app_config, )).start()
                line_count += 1
                if line_count % 10 == 0:
                    logger.info("THREAD-STATUS: There are {} currently active threads.".format(threading.activeCount()))
                # fin
            # fin

    except KeyboardInterrupt:
        pass
# end read_log_file


def parse_upload_file_line(line, logger, app_config):
    import datetime

    # Set Up
    base_dir = get_config_item(app_config, 'ftp_base_dir')

    start_timing = time.time()

    start_date = datetime.datetime.now()
    date_string = start_date.strftime('%Y') + "-" + start_date.strftime("%m") + "-" + start_date.strftime("%d")
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


def push_file_to_s3(logger, app_config, camera, date_part, hour_part, img_type, s3_file_name, local_file, start_timing):
    import boto3
    s3 = boto3.resource('s3')
    logging.getLogger('boto3').addHandler(logger)
    s3_object = get_config_item(app_config, 's3_info.object_base') + \
                camera + '/' + date_part + '/' + hour_part + '/' + img_type + '/' + s3_file_name
    s3.Object(get_config_item(app_config, 's3_info.bucket_name'), s3_object).put(Body=open(local_file, 'rb'))
    totaltime = time.time() - start_timing
    logger.info("S3 Object: {} written to s3 in {} seconds.".format(s3_object, totaltime))
    sys.exit(0)
# end push_file_to_s3


def transcodetomp4(file_in):

    import subprocess

    file_out = file_in.replace('.mkv', '.mp4')

    convert_command = '/usr/bin/avconv -i "{}" -f mp4 -vcodec copy -acodec libfaac -b:a 112k -ac 2 -y "{}"'\
        .format(file_in, file_out)

    try:
        subprocess.check_call(convert_command, shell=True)
    except subprocess.CalledProcessError:
        return file_in

    return file_out
# end transcodetomp4


def config_reader(config_file):
    if os.path.exists(config_file):
        with open(config_file, 'r') as cfile:
            app_config = json.load(cfile)
        # end with
        return app_config
    else:
        print("The config file: {} does not exist, please try again.".format(config_file))
        return False
    # fin

# end config_reader


def get_config_item(app_config, item):
    item_path = item.split('.')
    this_config = app_config
    for path_part in item_path:
        this_config = this_config[path_part]
    # end For

    return this_config
# end get_config_item

if __name__ == "__main__":
    main()
