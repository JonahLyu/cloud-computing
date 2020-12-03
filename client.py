from kazoo.client import KazooClient
import server, time, uuid
import numpy as np
import matplotlib.pyplot as plt

ELECTION_PATH="/master"
TASKS_PATH="/tasks"
PARAMS_PATH="/params"
WORKERS_PATH="/workers"
RESULTS_PATH="/results"
CLIENT_PATH="/clients"

zk = server.init()

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


def creatTask(sliceNum, width,height, zoom):
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

tasks = creatTask(sliceNum, width, height, zoom)

#create result path
newResultPath = f'{RESULTS_PATH}/{clientID}'
newResultPath = zk.ensure_path(newResultPath)
print("create: %s" % newResultPath)

for i in range(len(tasks)):
    # 1. create /params/task_id to store the parameters of task
    newParamPath = f'{PARAMS_PATH}/{i}_'
    newParamPath = zk.create(newParamPath, tasks[i], sequence=True, ephemeral=True)
    # 2. create /tasks/task_id
    taskID = newParamPath.split('/')[2]
    newTaskPath = f'{TASKS_PATH}/{taskID}'
    newTaskPath = zk.create(newTaskPath, clientID.encode("utf-8"), ephemeral=True)
    print("create: %s" % newParamPath)
    print("create: %s" % newTaskPath)
    

start = time.time()

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

results = zk.get_children(newResultPath)
for taskID in results: 
    data, _ = zk.get(f"{newResultPath}/{taskID}")
    paramsData, _ = zk.get(f"{PARAMS_PATH}/{taskID}")
    paramsData = paramsData.decode("utf-8")
    params = paramsData.split(':')
    startRow,  endRow= int(params[3]), int(params[4])
    pixelSlice = np.frombuffer(data, dtype=np.uint16).reshape(endRow-startRow,width)
    pixels[startRow:endRow] = pixelSlice
plt.axis('off')
plt.imshow(pixels)
plt.savefig("result.png")
while True:
    time.sleep(1)
