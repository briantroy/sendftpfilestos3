import threading
from tail import follow


def main():
    import sys
    base_dir = "/home/securityspy/security-images/alarm-images"
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
    line_parts = line.split(",")
    sys.stdout.write("File for upload is: " + line_parts[1] + " with file size: " + line_parts[2] + "\n")
    sys.exit(0)

if __name__ == "__main__":
    main()
