""" Tests for the fptfiletos3 """
import unittest
import sys
import os
import logging
import ftpfiletos3


class TestFtpFileToS3(unittest.TestCase):

    def test_app_config_error(self):
        sys.argv.remove('unit_tests')
        output1 = ftpfiletos3.check_config_file()
        sys.argv.append("not a file")
        output2 = ftpfiletos3.check_config_file()
        if not output1 and not output2:
            output = False
        else:
            output = True
        # fin
        self.assertFalse(output)
    # end test_app_config_error

    def test_get_config_item(self):
        this_config = {'foo': {'bar': 1}}
        output = ftpfiletos3.get_config_item(this_config, "foo.bar")
        self.assertEqual(this_config['foo']['bar'], output)
    # end test_get_config_item

    def test_bad_config(self):
        config_file = "not a file"
        output = ftpfiletos3.config_reader(config_file)
        self.assertFalse(output)
    # end test_bad_config

    def test_good_config(self):
        config_file = 'testdata/valid_config.json'
        output = ftpfiletos3.config_reader(config_file)
        test_data = output['ftp_base_dir']
        self.assertEqual(test_data, '/Volumes/internal-sd/Development/sendftpfilestos3')
    # end test_good_config

    def test_good_app_config(self):
        sys.argv[1] = 'testdata/valid_config.json'
        output = ftpfiletos3.check_config_file()
        test_data = output['ftp_base_dir']
        self.assertEqual(test_data, '/Volumes/internal-sd/Development/sendftpfilestos3')
    # end test_good_config

    def test_pid_file_create(self):
        this_config = {'app_pid_file': 'testdata/test.pid'}
        logger = logging.getLogger()
        output = ftpfiletos3.create_pid_file(this_config, logger)
        os.remove('testdata/test.pid')
        self.assertTrue(output)
    # end test_pid_file_create

    def test_bad_pid_file_create(self):
        this_config = {'app_pid_file': '/var/log/test.pid'}
        logger = logging.getLogger()
        output = ftpfiletos3.create_pid_file(this_config, logger)
        self.assertFalse(output)
    # end test_ad_pid_file_create

    def test_pid_exists(self):
        this_config = {'app_pid_file': 'testdata/test.pid'}
        open(this_config['app_pid_file'], 'a').close()
        logger = logging.getLogger()
        output = ftpfiletos3.create_pid_file(this_config, logger)
        os.remove(this_config['app_pid_file'])
        self.assertFalse(output)
    # end test_pid_exists

    def test_bad_logger_setup(self):
        sys.argv[1] = 'testdata/valid_config.json'
        this_config = ftpfiletos3.check_config_file()
        logger = ftpfiletos3.logger_setup(this_config)
        self.assertFalse(logger)
    # end test_bad_logger_setup

    def test_good_logger_setup(self):
        sys.argv[1] = 'testdata/valid_config.json'
        this_config = ftpfiletos3.check_config_file()
        this_config['app_log_file']['file'] = 'testdata/test_log_file.log'
        logger = ftpfiletos3.logger_setup(this_config)
        self.assertTrue(logger)
    # end test_good_logger_setup

    def test_log_file_reader(self):
        sys.argv[1] = 'testdata/valid_config.json'
        this_config = ftpfiletos3.check_config_file()
        this_config['app_log_file']['file'] = 'testdata/test_log_file.log'
        this_config['log_file_to_follow']['file'] = 'testdata/test_log_file.log'
        logger = ftpfiletos3.logger_setup(this_config)
        runit = ftpfiletos3.read_log_file(logger, this_config, True)
        self.assertTrue(runit)
    # end test_log_file_reader

    def test_log_file_reader_no_file(self):
        sys.argv[1] = 'testdata/valid_config.json'
        this_config = ftpfiletos3.check_config_file()
        this_config['app_log_file']['file'] = 'testdata/test_log_file.log'
        this_config['log_file_to_follow']['file'] = 'testdata/foo.log'
        logger = ftpfiletos3.logger_setup(this_config)
        runit = ftpfiletos3.read_log_file(logger, this_config, True)
        self.assertTrue(runit)
    # end test_log_file_reader_no_file

    def test_log_file_reader_small_file(self):
        sys.argv[1] = 'testdata/valid_config.json'
        this_config = ftpfiletos3.check_config_file()
        this_config['app_log_file']['file'] = 'testdata/test_log_file.log'
        this_config['log_file_to_follow']['file'] = 'testdata/small_log.log'
        logger = ftpfiletos3.logger_setup(this_config)
        runit = ftpfiletos3.read_log_file(logger, this_config, True)
        self.assertTrue(runit)
    # end test_log_file_reader_small_file

    def test_parse_log_line_with_video(self):
        log_line = 'Tue Aug 30 16:30:30 2016 [pid 10220] [securityspy] OK UPLOAD: ' +\
                   'Client "::ffff:192.168.0.89", ' +\
                   '"/Volumes/internal-sd/Development/sendftpfilestos3/testdata/' +\
                   'foo/record/MDalarm_20160830_152954.mkv", ' +\
                   '3885542 bytes, 129.01Kbyte/sec'

        sys.argv[1] = 'testdata/valid_config.json'
        this_config = ftpfiletos3.check_config_file()
        this_config['app_log_file']['file'] = 'testdata/test_log_file.log'
        this_config['log_file_to_follow']['line_identifier'] = 'OK UPLOAD'
        logger = ftpfiletos3.logger_setup(this_config)
        runit = ftpfiletos3.parse_upload_file_line(log_line, logger, this_config, True)
        self.assertTrue(runit)

    def test_parse_log_line_with_old_camera(self):
        log_line = '/var/log/vsftpd.log:Tue Aug 30 16:43:23 2016 [pid 18203] ' +\
                   '[securityspy] OK UPLOAD: Client "::ffff:192.168.0.72", ' +\
                   '"/home/securityspy/security-images/alarm-images/downdown/000DC5D4D0F7(downdown)' +\
                   '_1_20160830154350_5219.jpg", 30996 bytes, 70.52Kbyte/sec'
        sys.argv[1] = 'testdata/valid_config.json'
        this_config = ftpfiletos3.check_config_file()
        this_config['app_log_file']['file'] = 'testdata/test_log_file.log'
        this_config['log_file_to_follow']['line_identifier'] = 'OK UPLOAD'
        logger = ftpfiletos3.logger_setup(this_config)
        runit = ftpfiletos3.parse_upload_file_line(log_line, logger, this_config, True)
        self.assertTrue(runit)

    def test_parse_log_line_with_empty_file(self):
        log_line = '/var/log/vsftpd.log:,thisfile.foo,Kbyte/sec'
        sys.argv[1] = 'testdata/valid_config.json'
        this_config = ftpfiletos3.check_config_file()
        this_config['app_log_file']['file'] = 'testdata/test_log_file.log'
        this_config['log_file_to_follow']['line_identifier'] = 'OK UPLOAD'
        logger = ftpfiletos3.logger_setup(this_config)
        runit = ftpfiletos3.parse_upload_file_line(log_line, logger, this_config, True)
        self.assertTrue(runit)

# end class
