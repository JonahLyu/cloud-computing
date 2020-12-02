import time, socket, os, uuid, sys, kazoo, logging, signal
from kazoo.protocol.states import EventType
from kazoo.client import KazooClient
from election import Election
import server
ELECTION_PATH="/master"
TASKS_PATH="/tasks"
DATA_PATH="/data"
WORKERS_PATH="/workers"

class Worker:

    def __init__(self,zk):
        self.zk = zk
        #1.choose a random id
        self.uuid = uuid.uuid4()
        self.path = f'{WORKERS_PATH}/{self.uuid}'
        #2.create znode
        self.worker_id = zk.create(self.path, ephemeral=True)
        zk.set(self.worker_id, b"non")
        print("Worker %s  created!" %(self.worker_id))
        #3.watch znode
        zk.DataWatch(self.path, self.assignment_change)   
    
    # do something upon the change on assignment
    def assignment_change(self, atask, stat):
        if atask and not atask.decode("utf-8") == "non" :
            atask = atask.decode("utf-8")
            #4.5. get task id uppon assignment in workers, get task data in data/yyy
            data_path = f'{DATA_PATH}/{atask}'
            if self.zk.exists(data_path) :
                data, _ = self.zk.get(data_path)
                #6. execute task with data
                time.sleep(1)
                result = data.decode("utf-8")
                task_path = f'{TASKS_PATH}/{atask}'
                task_val = atask + "#" + result
                # set result in task - task completion
                if self.zk.exists(task_path) :
                    zk.set(task_path, task_val.encode('utf-8'))
                    print("Worker completed task %s with result %s" %(atask, result))
                else :
                    print("Task %s not found, maybe the connection  was lost" %(task_path))
                #7. delete assignment
                zk.set(self.path, b"non")

if __name__ == '__main__':
    zk = server.init()    
    worker = Worker(zk)
    while True:
        time.sleep(1)