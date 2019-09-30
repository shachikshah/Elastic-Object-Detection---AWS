import boto.sqs
import json
import boto3
import os
import time

ACCESS_KEY_ID = ''
SECRET_ID = ''
conn = boto.sqs.connect_to_region('us-west-1', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ID)
queue = conn.get_queue('cc-videos-queue')
msg = ''

print('inside getDataFromSQS')


def getVideonameFromSQS():
    global msg
    print('getting video from SQS')
    rs = ''
    x = 0
    while len(rs) == 0:
        x = x + 1
        rs = queue.get_messages()
        print(x)
        print('got message as')
        print(rs)

        if len(rs) == 0:
            time.sleep(20)
            print('slept once')
            if x == 1:
                os.system('sudo shutdown now')

    m = rs[0]
    string_out = m.get_body()
    msg = string_out
    acceptable_out = string_out.replace("'", "\"")
    json_out = json.loads(acceptable_out)
    video_name = json_out['Records'][0]['s3']['object']['key']

    # Flagged Code
    downloadFromS3(video_name)
    queue.delete_message(m)
    return video_name


def downloadFromS3(video_name):
    path = '/home/ubuntu/darknet/videos/' + video_name
    client = boto3.client('s3', aws_access_key_id=ACCESS_KEY_ID,
                          aws_secret_access_key=SECRET_ID,
                          aws_session_token='')
    BUCKET_NAME = 'cc-videos-s3-bucket'
    client.download_file(BUCKET_NAME, video_name, path)


def writeFilename(video_name):
    f = open('/home/ubuntu/darknet/video_name.txt', 'w+')
    f.write(video_name)
    f.close()


if __name__ == '__main__':
    try:
        video_name = getVideonameFromSQS()
        writeFilename(video_name)
    except:
        if len(msg) != 0 and msg != '':
            conn.send_message(queue, msg)
            time.sleep(5)
        os.system('sudo shutdown now')

# Logic to remove downloaded video from instance.
# Here or maybe in darket_test.py

