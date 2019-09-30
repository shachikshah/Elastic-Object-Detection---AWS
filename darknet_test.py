#!/usr/bin/python
import os
import sys
import boto3
import time


def pushToS3(filename):
    ACCESS_KEY = ''
    SECRET_KEY = ''

    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY,
                             aws_session_token='')
    BUCKET_NAME = 'cc-videos-output'
    s3_client.upload_file(filename, BUCKET_NAME, filename)
    print('Uploaded to S3')


def refactorOutput(filename):
    file_out = open(filename, 'r+')
    text = file_out.read()


# weight_path = sys.argv[0]
def continue_or_stop():
    ACCESS_KEY = ''
    SECRET_KEY = ''

    queue_url = 'https://sqs.us-west-1.amazonaws.com/672371277498/cc-videos-queue'

    sqs_client = boto3.client('sqs', aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY,
                              aws_session_token='', region_name='us-west-1')

    attr = sqs_client.get_queue_attributes(QueueUrl=queue_url,
                                           AttributeNames=['ApproximateNumberOfMessages'])['Attributes']

    msg_count = int(attr['ApproximateNumberOfMessages'])
    # print(msg_count)
    if msg_count == 0:
        # print('count is 0')
        # code to stop server
        os.system('sudo shutdown now')
    else:
        # print('not zero, running again')
        # code to pickup another video and process.
        os.system('/bin/bash /home/ubuntu/init.sh')


# video_path = sys.argv[1]
obj = set()
# cmd = "./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg "+ weight_path + " " + video_path + " -dont_show > result"
# os.system(cmd)
file_in = open("result", "r")
for lines in file_in:
    if lines.split(':')[0] == 'video file':
        path = lines.split(':')[1].strip()
    if lines == "\n":
        continue
    if lines.split(":")[-1] == "\n":
        continue
    if lines.split(":")[-1][-2] == "%":
        obj.add(lines.split(":")[0])
try:
    name = path.split('/')[-1]

except:
    name = 'default'
filename = name + '_output'
file_out = open(filename, "w+")
file_out.write('(' + name + ' - ')
for items in obj:
    file_out.write(str(items))
    file_out.write(",")

if obj.__len__() == 0:
    file_out.write(', No item is detected')
file_out.write(')')
file_in.close()
file_out.close()

try:
    refactorOutput(filename)
    pushToS3(filename)
    continue_or_stop()
except:
    os.system('sudo shutdown now')

