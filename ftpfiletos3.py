from tail import follow


def main():
    base_dir = "/home/securityspy/security-images/alarm-images"
    ftp_log_file = "/var/log/vsftpd.log"

    with open(ftp_log_file, 'rt') as following:
        following.seek(-64, 2)
        try:
            for line in follow(following):
                sys.stdout.write(line)
        except KeyboardInterrupt:
            pass

