import time, socket, os, uuid, sys, kazoo, logging, signal
from kazoo.protocol.states import EventType
from kazoo.client import KazooClient
from election import Election
import server
ELECTION_PATH="/master"
TASKS_PATH="/tasks"
DATA_PATH="/data"
WORKERS_PATH="/workers"

class Master:
    #initialize the master
    def __init__(self,zk):
        self.master = False
        self.zk = zk
        my_path = zk.create(ELECTION_PATH + '/id_', ephemeral=True, sequence=True)
        self.election = Election(zk, ELECTION_PATH, my_path)
        self.election.ballot(self.zk.get_children(ELECTION_PATH))
        zk.ChildrenWatch(WORKERS_PATH, self.distribute_task, send_event=True)
        zk.ChildrenWatch(TASKS_PATH, self.distribute_task, send_event=True)

    def compute_free_worker(self):
        workers = self.zk.get_children(WORKERS_PATH)
        if not workers == None : 
            for i in range(0, len(workers)) :
                worker_path = f'{WORKERS_PATH}/{workers[i]}'
                val, _ = self.zk.get(worker_path)
                # Check if no task is assigned to the worker
                if ("non" in val.decode("utf-8")) :
                    return  workers[i]
        return None

    #assign tasks 				   
    def distribute_task(self, children, event):
        if self.election.is_master_server():
            if(event) :
       	        print("Change happened with event  = %s" %(event.type))
            tasks = self.zk.get_children(TASKS_PATH) #get all tasks
            for i in range(0,len(tasks)) :
                task_path = f'{TASKS_PATH}/{tasks[i]}'
                task_raw_data, _ = self.zk.get(task_path)
                task_data = ''
                if task_raw_data is not None:
                    task_data = task_raw_data.decode("utf-8")
                if ("#" not in task_data) : # not assigned
                    free_worker = self.compute_free_worker()
                    print("found free worker: %s" % free_worker)
                    if not free_worker == None :
                        worker_path = f'{WORKERS_PATH}/{free_worker}'
                        print("Assigned worker = %s to task = %s" %(free_worker, tasks[i]))
                        new_task_data = f'{task_data}#{free_worker}'
                        self.zk.set(task_path, new_task_data.encode("utf-8"))
                        self.zk.set(worker_path, tasks[i].encode("utf-8"))
                        self.zk.get(worker_path, self.task_complete)
			
    def task_complete(self, event) :
        # a worker finished a task
        data, _ = zk.get(event.path)
        if data is not None: 
            if data.decode("utf-8") == "non":
                print("complete task: %s" % data.decode("utf-8"))
				
                
if __name__ == '__main__':
    zk = server.init()
    master = Master(zk)
    while True:
        time.sleep(1)

