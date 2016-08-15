import sys, threading
from tail import follow


def main():
    base_dir = "/home/securityspy/security-images/alarm-images"
    ftp_log_file = "/var/log/vsftpd.log"

    with open(ftp_log_file, 'rt') as following:
        following.seek(-64, 2)
        try:
            for line in follow(following):
                if "OK UPLOAD" in line:
                    sys.stdout.write(line)
                    t = threading.Thread(target=parse_upload_file_line, args=(line, )).start()
        except KeyboardInterrupt:
            pass

def parse_upload_file_line(line):
    line_parts = line.split(",")
    sys.stdout.write(line_parts[2])

if __name__ == "__main__":
    main()
