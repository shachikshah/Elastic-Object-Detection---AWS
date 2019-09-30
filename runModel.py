import os


def runModel(video_name):
    os.chdir('/home/ubuntu/darknet')
    darknet_detector = './darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights videos/' + video_name + ' -dont_show > result'
    os.system(darknet_detector)
    os.system('python /home/ubuntu/darknet/darknet_test.py')


# os.remove('video_name.txt')

with open('/home/ubuntu/darknet/video_name.txt') as f:
    try:
        video = f.read()
        runModel(video)
    except:
        os.system('sudo shutdown now')

