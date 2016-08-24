import threading
from tail import follow
import logging
import logging.handlers
import os
import sys
import time


def main():

    pid = str(os.getpid())
    pidfile = "/tmp/ftpfilestos3.pid"

    # set up logger
    app_log_file = "/var/log/securitys3uploader.log"

    app_logger = logging.getLogger('AppLogger')
    app_logger.setLevel(logging.DEBUG)

    # Add the log message handler to the logger
    handler = logging.handlers.RotatingFileHandler(
        app_log_file, maxBytes=5242880, backupCount=4)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(thread)d - %(threadName)s - %(message)s')
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

    t = threading.Thread(name='log-reader', target=read_log_file, args=(app_logger,)).start()

# end Main


def read_log_file(logger):

    ftp_log_file = "/var/log/vsftpd.log"
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
        for line in follow(fstream):
            if "OK UPLOAD" in line:
                thread_name = 'line-handler-' + str(line_count)
                t = threading.Thread(name=thread_name, target=parse_upload_file_line, args=(line, logger,)).start()
                line_count += 1
    except KeyboardInterrupt:
        pass
# end read_log_file


def parse_upload_file_line(line, logger):
    import boto3
    import datetime

    # Set Up
    base_dir = "/home/securityspy/security-images/alarm-images"

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

    s3 = boto3.resource('s3')
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

    s3_object = 'patrolcams/' + path_parts[1] + '/' + date_string + '/' + hour_string + '/' + img_type + '/' + just_file
    s3.Object('security-alarms', s3_object).put(Body=open(file_name, 'rb'))
    totaltime = time.time() - start_timing
    logger.info("S3 Object: {} written to s3 in {} seconds.".format(s3_object, totaltime))
    sys.exit(0)
# end parse_upload_file_line


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


if __name__ == "__main__":
    main()
