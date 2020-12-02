from kazoo.client import KazooClient
import server, time
import numpy as np

ELECTION_PATH="/master"
TASKS_PATH="/tasks"
PARAMS_PATH="/params"
WORKERS_PATH="/workers"
RESULTS_PATH="/results"

zk = server.init()

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

for i in range(len(tasks)):
    # 1. create /params/task_id to store the parameters of task
    newDataPath = f'{PARAMS_PATH}/{i}_'
    newDataPath = zk.create(newDataPath, tasks[i], sequence=True, ephemeral=True)
    # 2. create /tasks/task_id
    taskID = newDataPath.split('/')[2]
    newTaskPath = f'{TASKS_PATH}/{taskID}'
    newResultPath = f'{RESULTS_PATH}/{taskID}'
    newTaskPath = zk.create(newTaskPath, ephemeral=True)
    newResultPath = zk.create(newResultPath, ephemeral=True)
    print(newDataPath)
    print(newTaskPath)
    print(newResultPath)

while True:
    time.sleep(1)
