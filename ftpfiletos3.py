import threading
from tail import follow


def main():
    import sys
    ftp_log_file = "/var/log/vsftpd.log"

    with open(ftp_log_file, 'rt') as following:
        following.seek(-64, 2)
        try:
            for line in follow(following):
                if "OK UPLOAD" in line:
                    t = threading.Thread(target=parse_upload_file_line, args=(line, )).start()
        except KeyboardInterrupt:
            pass

def parse_upload_file_line(line):
    import sys
    import boto3
    from datetime import date, timedelta
    start_date = date.today()
    date_string = start_date.strftime('%Y') + "-" + start_date.strftime("%m") + "-" + start_date.strftime("%d")
    hour_string = "Hour-" + start_date.hour
    base_dir = "/home/securityspy/security-images/alarm-images"
    line_parts = line.split(",")
    sys.stdout.write("File for upload is: " + line_parts[1] + " with file size: " + line_parts[2] + "\n")
    s3 = boto3.resource('s3')
    # Parse the file name to get the sub-folder and object name
    path_end = line_parts[1].replace(base_dir, "")
    path_parts = path_end.split('/')
    sys.stdout.write("File of type: " + path_parts[3] + " for camera " + path_parts[1] + " with file name " + path_parts[4] + "\n")
    s3_object = '/' + path_parts[1] + '/' + date_string + '/' + hour_string + '/' + path_parts[1] + '/' + path_parts[4]
    s3.Object('security-alarms', s3_object).put(Body=open(line_parts[1], 'rb'))
    sys.exit(0)

if __name__ == "__main__":
    main()
