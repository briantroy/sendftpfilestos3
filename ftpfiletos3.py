import threading
from tail import follow

def main():
    import signal
    import os
    import sys

    def signal_handler(signal, frame):
        if signal == 1:
            print("caught interrupt: " + str(signal) + " - restarting processing.")
            t = threading.Thread(target=read_log_file).start()
        elif signal == 9:
            print("caught kill signal... exiting...")
            os.remove("/tmp/ftpfilestos3.pid")
            sys.exit()
    # end signal_handler

    pid = str(os.getpid())
    pidfile = "/tmp/ftpfilestos3.pid"

    if os.path.isfile(pidfile):
        print("{} already exists, exiting".format(pidfile))
        sys.exit()
    with (open(pidfile, 'w')) as pidfilestream:
        pidfilestream.write(pid)
        pidfilestream.close()
    # end with

    signal.signal(signal.SIGHUP, signal_handler)

    t = threading.Thread(target=read_log_file).start()

# end Main


def read_log_file():
    ftp_log_file = "/var/log/vsftpd.log"
    fstream = open(ftp_log_file, "rt")
    fstream.seek(-64, 2)
    try:
        for line in follow(fstream):
            if "OK UPLOAD" in line:
                t = threading.Thread(target=parse_upload_file_line, args=(line,)).start()
    except KeyboardInterrupt:
        pass
# end read_log_file


def parse_upload_file_line(line):
    import sys
    import boto3
    import logging
    import time
    from datetime import datetime, date, timedelta

    # @todo
    # convert the file to mp4 before uploading using
    # avconv -i ./MDalarm_20160819_105607.mkv -f mp4 -vcodec copy -acodec libfaac -b:a 112k -ac 2 -y ~/outfile.mp4

    # Set Up
    base_dir = "/home/securityspy/security-images/alarm-images"
    logging.basicConfig(filename="securitys3uploader.log", level=logging.INFO)

    start_timing = time.time()

    start_date = datetime.now()
    date_string = start_date.strftime('%Y') + "-" + start_date.strftime("%m") + "-" + start_date.strftime("%d")
    hour_string = "Hour-" + str(start_date.hour)

    line_parts = line.split(",")
    file_name = line_parts[1].strip()
    file_name = file_name.replace('"', '')
    logging.info("File for upload is: {} with file size: {}".format(file_name, line_parts[2]))
    if line_parts[2].find('Kbyte/sec') != -1:
        logging.info("Skippking file {} because it is empty.".format(file_name))
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
            logging.error("File {} could not be transcoded to mp4.".format(file_name))
            sys.exit(0)
        #fin
    #fin

    s3_object = 'patrolcams/' + path_parts[1] + '/' + date_string + '/' + hour_string + '/' + img_type + '/' + just_file
    s3.Object('security-alarms', s3_object).put(Body=open(file_name, 'rb'))
    totaltime = time.time() - start_timing
    logging.info("S3 Object: {} written to s3 in {} seconds.".format(s3_object, totaltime))
    sys.exit(0)

def transcodetomp4(file_in):

    import subprocess
    # @todo
    # convert the file to mp4 before uploading using
    # avconv -i ./MDalarm_20160819_105607.mkv -f mp4 -vcodec copy -acodec libfaac -b:a 112k -ac 2 -y ~/outfile.mp4

    file_out = file_in.replace('.mkv', '.mp4')

    convert_command = '/usr/bin/avconv -i "{}" -f mp4 -vcodec copy -acodec libfaac -b:a 112k -ac 2 -y "{}"'.format(file_in, file_out)

    try:
        subprocess.check_call(convert_command, shell=True)
    except subprocess.CalledProcessError:
        return file_in

    return file_out


if __name__ == "__main__":
    main()
