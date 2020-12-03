from kazoo.client import KazooClient
import time, uuid, signal,sys
import numpy as np
import matplotlib.pyplot as plt

import yaml

ELECTION_PATH="/master"
TASKS_PATH="/tasks"
PARAMS_PATH="/params"
WORKERS_PATH="/workers"
RESULTS_PATH="/results"
CLIENT_PATH="/clients"

with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

hosts = [(k, v) for k, v in config['hosts'].items()]

cluster_ip = list(config['hosts'].keys())[0]

zk = KazooClient(hosts=f'{cluster_ip}:2181')
zk.start()

# close the zk connection with Ctrl + c signal
def interruptHandler(signal, frame):
    zk.stop()
    sys.exit(0)
# handle interrupt signal 
signal.signal(signal.SIGINT, interruptHandler)

clientID = "client_" + str(uuid.uuid4())
zk.create(f'{CLIENT_PATH}/{clientID}', ephemeral=True)

count = 0
pixels = []
sliceNum = 3
width = 1024
height = 768
zoom = 1

# watch the workers status
@zk.ChildrenWatch(WORKERS_PATH)
def watch_worker(workers):
    print("%d workers: %s" % (len(workers), workers))


# watch the master status
@zk.ChildrenWatch(ELECTION_PATH)
def watch_master(masters):
    print("%d masters: %s" % (len(masters), masters))


#create result path
newResultPath = f'{RESULTS_PATH}/{clientID}'
newResultPath = zk.ensure_path(newResultPath)
print("create: %s" % newResultPath)

sliceNum = int(input("enter number of slices to deploy: "))
zoom = float(input("enter zoom (float value): "))


def generateTask(sliceNum, width,height, zoom):
    global pixels
    pixels = np.arange(width*height, dtype=np.uint16).reshape(height, width)
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
    return allRows

tasks = generateTask(sliceNum, width, height, zoom)

start = time.time()
# deploy tasks
for i in range(len(tasks)):
    # 1. create /params/task_id to store the parameters of task
    newParamPath = f'{PARAMS_PATH}/{i}_'
    newParamPath = zk.create(newParamPath, tasks[i], sequence=True, ephemeral=True)
    # 2. create /tasks/task_id
    taskID = newParamPath.split('/')[2]
    newTaskPath = f'{TASKS_PATH}/{taskID}'
    newTaskPath = zk.create(newTaskPath, clientID.encode("utf-8"), ephemeral=True)
    print("deploy task: %s %s" % (newTaskPath, tasks[i]))
    

# watch the result status
@zk.ChildrenWatch(newResultPath)
def watch_result(results):
    global pixels, count, width
    count = len(results)
    print("task progress: %d/%d" % (count, sliceNum))


while count < sliceNum:
    pass
end = time.time()
print("Total time: %.2f seconds" % (end - start))

print("download image...")
count = 0
results = zk.get_children(newResultPath)
for taskID in results: 
    data, _ = zk.get(f"{newResultPath}/{taskID}")
    paramsData, _ = zk.get(f"{PARAMS_PATH}/{taskID}")
    paramsData = paramsData.decode("utf-8")
    params = paramsData.split(':')
    startRow,  endRow= int(params[3]), int(params[4])
    pixelSlice = np.frombuffer(data, dtype=np.uint16).reshape(endRow-startRow,width)
    pixels[startRow:endRow] = pixelSlice
    count += 1
    print("download progress: %d/%d" % (count, sliceNum))
plt.axis('off')
plt.imshow(pixels)
plt.savefig("result.png")
print("image saved as result.png")
input("enter to quit: ")
zk.stop()