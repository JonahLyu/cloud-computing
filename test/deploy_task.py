from typing import ByteString
import yaml, time
import numpy as np
import matplotlib.pyplot as plt


with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# start watching
from kazoo.client import KazooClient
import logging

logging.basicConfig()

cluster_ip = list(config['hosts'].keys())[0]

zk = KazooClient(hosts=f'{cluster_ip}:2181')

zk.start()
count = 0
pixels = []
sliceNum = 9
width = 1024
height = 768
zoom = 1.2


# watch the server status
@zk.ChildrenWatch("/servers")
def watch_server(servers):
    print("servers are %s" % servers)


def creatTask(sliceNum, width,height, zoom):
    global pixels
    pixels = np.arange(width*height, dtype=np.uint16).reshape(height, width)
    taskQueue = zk.LockingQueue("/taskQueue")
    allRows = []
    for i in range(sliceNum):
        startRow = (height // sliceNum) * i
        if i == sliceNum - 1:
            endRow = (height // sliceNum) * (i + 1) + height % sliceNum
        else:
            endRow = (height // sliceNum) * (i + 1)
        task = f'{width}:{height}:{zoom}:{startRow}:{endRow}'
        allRows.append(task.encode("utf-8"))
    print("deploy tasks: ", sliceNum)
    taskQueue.put_all(allRows)

creatTask(sliceNum, width, height, zoom)
start = time.time()

# watch the app data status
@zk.ChildrenWatch("/data")
def watch_slice(slices):
    global pixels, count, width
    for key in slices: 
        data, _ = zk.get(f"/data/{key}")
        params = key.split(':')
        startRow,  endRow= int(params[0]), int(params[1])
        pixelSlice = np.frombuffer(data, dtype=np.uint16).reshape(endRow-startRow,width)
        pixels[startRow:endRow] = pixelSlice
        zk.delete(f"/data/{key}")
        count += 1
        print(f'{key} finished {count}/{sliceNum}')



while count < sliceNum:
    pass
end = time.time()
print("Total time: %.2f seconds" % (end - start))
# input("wait to quit:\n")
plt.axis('off')
plt.imshow(pixels)
plt.savefig("result.png")
# plt.show()
input("wait to quit:\n")
zk.stop()