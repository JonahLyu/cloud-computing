from kazoo.client import KazooClient
import server, time, uuid
import numpy as np

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
zoom = 1.2

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

for i in range(len(tasks)):
    # 1. create /params/task_id to store the parameters of task
    newParamPath = f'{PARAMS_PATH}/{i}_'
    newParamPath = zk.create(newParamPath, tasks[i], sequence=True, ephemeral=True)
    # 2. create /tasks/task_id
    taskID = newParamPath.split('/')[2]
    newTaskPath = f'{TASKS_PATH}/{taskID}'
    newTaskPath = zk.create(newTaskPath, clientID.encode("utf-8"), ephemeral=True)
    print(newParamPath)
    print(newTaskPath)
    

# watch the result status
@zk.ChildrenWatch(newResultPath)
def watch_result(results):
    print("results: %s" % (results))

print(newResultPath)
while True:
    time.sleep(1)
