# sendftpfilestos3
Sends files transfered via FTP on a secure network to S3.

The genesys of this project was to have off-site storage of security camera alarms - i.e., motion detection events. The goal was near real time upload.

The original project (now quite old, but can be found here: https://github.com/briantroy/Zoneminder-Alert-Image-Upload-to-Amazon-S3) used node.js and relied on Zoneminder. This project decouples the upload from Zoneminder and allows any camera that can do a simple FTP upload to have files pushed to Amazon S3.
A single RaspberryPi powers both my internal FTP server and runs the Python script found in this repo.

In addition, I wanted a simple catalog of the capture events and an API for serving up the events to a web application. This is accomplished using the AWS Lambda functions found in this repository.
The Web application - built to consume those APIs - can be found here:
https://github.com/briantroy/SecurityVideos

These two repositories form a complete solution for uploading videos from an FTP server to S3, processing/cataloging them in DynamoDB using a Lambda function and generating a secured REST API using Lambda functions with AWS API Gateway.

A complete write-up of these two projects is coming soon...
