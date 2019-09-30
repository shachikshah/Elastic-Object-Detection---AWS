
ACCESS_ID = ""
ACCESS_KEY = ""
REGION = ""


from flask import Flask, session
# from flask.ext.session import Session
import boto3
import cgi
import sys
import os
import time
import requests
import ast
import time
import random
from botocore.exceptions import ClientError

app = Flask(__name__)

s3_client = boto3.client('s3', aws_access_key_id=ACCESS_ID2,
                         aws_secret_access_key=ACCESS_KEY2,
                         aws_session_token='', region_name=REGION)

sqs_client = boto3.client('sqs', aws_access_key_id=ACCESS_ID2,
                          aws_secret_access_key=ACCESS_KEY2,
                          aws_session_token='', region_name=REGION)

ec2_res = boto3.resource('ec2', aws_access_key_id=ACCESS_ID2,
                         aws_secret_access_key=ACCESS_KEY2,
                         region_name=REGION)

s3_res = boto3.resource('s3', aws_access_key_id=ACCESS_ID2,
                        aws_secret_access_key=ACCESS_KEY2,
                        region_name=REGION)

# cc-videos-s3-bucket-avi
# cc-videos-s3-bucket
BUCKET_NAME = 'cc-videos-s3-bucket'

queue_url = 'https://sqs.us-west-1.amazonaws.com/672371277498/cc-videos-queue'


def start_slave(instance_id, ec2):
    try:
        ec2.instances.filter(InstanceIds=[instance_id]).start()
    except ClientError as e:
        print('Started Max Instance Limit')


def stop_slave(instance_id, ec2):
    try:
        ec2.instances.filter(InstanceIds=[instance_id]).stop()
    except ClientError as e:
        print('All Instances Dead')


# Code to terminate slaves, execute from console after demo
# ec2.instances.filter(InstanceIds=ids).terminate()
def create_slaves():
    instance = None
    count_running = get_running()[1]
    if count_running < 19:
        try:
                instance = ec2_res.create_instances(ImageId='ami-07919ea55bdb49fbe', MinCount=1, MaxCount=1,
                                                InstanceType='t2.micro')

        except ClientError as e:
            print('Reached Max Instance Limit')

    if instance:
        instance[0].create_tags(Tags=[{'Key': 'Name', 'Value': 'app-instance'}])


def get_video():
    r, video_name = request_pi()
    if r is None:
        return None
    downloadFile(r, video_name)
    pushFileToS3(video_name)
    removeFileFromLocal(video_name)
    return video_name


def request_pi():
    url_pi = "http://206.207.50.7/getvideo"
    r = requests.get(url_pi)
    if r.status_code == 200:
        video_name = getFileName(r)
        tmp = video_name.split('.')
        video_name = tmp[0]+"-"+str(time.time()).split('.')[1][-3:]+'.'+tmp[1]
        print('Connection to RaspberryPi established')
        return r, video_name
    else:
        return None, None


def getFileName(r):
    header = r.headers['content-disposition']
    value, params = cgi.parse_header(header)
    return params['filename']


def downloadFile(r, video_name):
    with open(video_name, 'wb') as video:
        video.write(r.content)
    print(video_name + " downloaded")


def pushFileToS3(video_name):
    # s3_client.create_bucket(Bucket=BUCKET_NAME)
    print(video_name + ' starting upload')

    Failed = True
    while Failed:
        try:
            s3_client.upload_file(video_name, BUCKET_NAME, video_name)
            Failed = False
        except:
            print(video_name, ' failed, retrying')
            Failed = True

    print(video_name + " uploaded")


def removeFileFromLocal(video_name):
    if os.path.exists(video_name):
        os.remove(video_name)



def get_running():
    running_slaves = []

    r_instances = ec2_res.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])

    for instance in r_instances:
        # print(instance.id, instance.tags[0]['Value'])
        if instance.tags[0]['Value'] != "web-instance1":
            running_slaves.append(instance.id)

    return running_slaves , len(running_slaves)


def get_stopped():
    stopped_slaves = []

    s_instances = ec2_res.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['stopped']}])

    for instance in s_instances:
        if instance.tags[0]['Value'] != "web-instance1":
            stopped_slaves.append(instance.id)

    return stopped_slaves , len(stopped_slaves)


@app.route('/')
def process_request():
    try:
        # get video from pi cluster, save it to s3 bucket 'cc-videos-s3-bucket' add its entry to SQS
        video_name = get_video()

        if video_name is None or len(video_name) <= 0:
            return 'No video received'

        stopped_slaves, count_stopped = get_stopped()

        x = 0
        if count_stopped > 1:
            x = random.randint(0, count_stopped-1)

        if stopped_slaves:
            instance_id = stopped_slaves.pop(x)
            start_slave(instance_id, ec2_res)
            print(video_name + ' started new worker: ' + instance_id)

        else:
            # Try to create new worker if already reached limit do nothing
            create_slaves()

        print(video_name + ' processing')

        # Code to poll result S3 output
        time.sleep(60)
        while True:
            time.sleep(30)
            bucket = s3_res.Bucket('cc-videos-output')
            for obj in bucket.objects.filter(Prefix='video'):
                # print('Outputs found: ', obj.key)
                if video_name in obj.key:
                    print(video_name, ' finished')
                    return obj.get()['Body'].read()

    except Exception as e:
        return str(e)+" Error"


if __name__ == '__main__':
      app.run()

