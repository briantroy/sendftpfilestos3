#!/bin/bash

/bin/kill $(cat /tmp/ftpfilestos3.pid)
/bin/rm -Rf /tmp/ftpfilestos3.pid
sleep(2)
/usr/bin/python /home/brian.roy/brian-extra/sendftpfilestos3/ftpfilestos3.py