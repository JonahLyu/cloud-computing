import time, socket, os, uuid, sys, kazoo, logging, signal
from kazoo.protocol.states import EventType
from kazoo.client import KazooClient
from election import Election
import server
ELECTION_PATH="/master"
TASKS_PATH="/tasks"
PARAMS_PATH="/params"
WORKERS_PATH="/workers"
STATUS_PATH="/status"
RESULTS_PATH="/results"

class Worker:

    def __init__(self,zk):
        self.zk = zk
        #generate random worker id
        self.workerID = uuid.uuid4()
        self.workerPath = f'{WORKERS_PATH}/{self.workerID}'
        self.statusPath = f'{STATUS_PATH}/{self.workerID}'
        #2.create znode
        zk.ensure_path(self.statusPath) # permanent status path
        zk.set(self.statusPath, b"non")
        zk.create(self.workerPath, b"non", ephemeral=True)
        print("Worker %s  created!" %(self.workerPath))
        #3.watch znode
        zk.DataWatch(self.statusPath, self.assignment_change)   
    
    # do something upon the change on assignment
    def assignment_change(self, taskID, stat):
        if taskID is not None and taskID.decode("utf-8") != "non":
            taskID = taskID.decode("utf-8")
            print("Worker recieved task %s" % taskID) 
            #4.5. get the parameters of the task
            paramPath = f'{PARAMS_PATH}/{taskID}'
            if self.zk.exists(paramPath) :
                data, _ = self.zk.get(paramPath)
                #6. execute task with data
                result = data.decode("utf-8")
                time.sleep(10)
                taskPath = f'{TASKS_PATH}/{taskID}'
                resultPath = f'{RESULTS_PATH}/{taskID}'
                # write result back to task
                if self.zk.exists(taskPath) and self.zk.exists(resultPath) :
                    zk.set(resultPath, result.encode("utf-8"))
                    zk.set(taskPath, b"complete")
                    #free this worker
                    zk.set(self.statusPath, b"non")
                    print("Worker completed task %s" % taskID)
                else :
                    #free this worker
                    zk.set(self.statusPath, b"non")
                    print("Task %s not found, maybe the connection  was lost" %(resultPath))

if __name__ == '__main__':
    zk = server.init()    
    worker = Worker(zk)
    while True:
        time.sleep(1)