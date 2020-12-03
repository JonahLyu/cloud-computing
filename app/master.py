from election import Election
import time, server, logging
ELECTION_PATH="/master"
TASKS_PATH="/tasks"
PARAMS_PATH="/params"
WORKERS_PATH="/workers"
STATUS_PATH="/status"
RESULTS_PATH="/results"
CLIENT_PATH="/clients"

class Master:
    # initialize the master
    def __init__(self,zk):
        self.master = False
        self.zk = zk
        self.workers = []
        self.clients = []

        # register itself as master election candidates
        myPath = zk.create(ELECTION_PATH + '/id_', ephemeral=True, sequence=True)

        # join the election of master
        self.election = Election(zk, ELECTION_PATH, myPath)
        self.election.ballorMaster(self.zk.get_children(ELECTION_PATH))

        # all election will watch
        zk.ChildrenWatch(WORKERS_PATH, self.onWorkerChange, send_event=True)
        zk.ChildrenWatch(TASKS_PATH, self.distributeTask, send_event=True)
        zk.ChildrenWatch(CLIENT_PATH, self.onClientChange, send_event=True)

    def findFreeWorker(self):
        workers = self.zk.get_children(WORKERS_PATH)
        if not workers == None : 
            for i in range(0, len(workers)) :
                statusPath = f'{STATUS_PATH}/{workers[i]}'
                val, _ = self.zk.get(statusPath)
                # Check if no task is assigned to the worker
                if ("non" in val.decode("utf-8")) :
                    return  workers[i]
        return None

    #distribute tasks to workers 				   
    def distributeTask(self, children=None, event=None):
        if self.election.isMasterServer():
            if(event) :
       	        logging.info("Change happened with event  = %s" %(event.type))
            tasks = self.zk.get_children(TASKS_PATH) #get all tasks
            for i in range(0,len(tasks)) :
                taskPath = f'{TASKS_PATH}/{tasks[i]}'
                taskStatus, _ = self.zk.get(taskPath)
                taskStatus = taskStatus.decode("utf-8")
                if ("assigned" not in taskStatus and "complete" not in taskStatus) : # not assigned
                    freeWorker = self.findFreeWorker()
                    logging.info("Try to distribut %s to worker: %s" % (tasks[i], freeWorker))
                    if freeWorker is None :
                        logging.info("There is not any free worker now")
                        break
                    else:
                        statusPath = f'{STATUS_PATH}/{freeWorker}'
                        clientID, _ = self.zk.get(taskPath)
                        clientID = clientID.decode("utf-8")
                        newTaskData = f'assigned#{clientID}#{freeWorker}'
                        self.zk.set(taskPath, newTaskData.encode("utf-8")) # store worker id into task status
                        self.zk.set(statusPath, tasks[i].encode("utf-8")) # store task into worker status
                        self.zk.get(statusPath, self.onTaskComplete)
			
    def onTaskComplete(self, event) :
        if self.election.isMasterServer():
            # a worker finished a task
            statusPath = event.path
            if self.zk.exists(statusPath):
                status, _ = zk.get(statusPath)
                if status is not None and status.decode("utf-8") == "non":
                    logging.info("worker task complete: %s" % statusPath)
                    self.distributeTask(event=event)

    def onWorkerChange(self, workers, event):
        if self.election.isMasterServer():
            # calculate died worker
            diedWorkers = list(set(self.workers) - set(workers))
            if len(diedWorkers) > 0:
                # free all tasks of died worker
                for diedWorker in diedWorkers:
                    data, _ = self.zk.get(f'{STATUS_PATH}/{diedWorker}')
                    status = data.decode("utf-8")
                    logging.info("Worker died %s with status: %s" % (diedWorker, status))
                    #free the task assiged to this died worker
                    if status != "non" and self.zk.exists(f'{TASKS_PATH}/{status}'): 
                        self.zk.set(f'{TASKS_PATH}/{status}', b'non') 
                        self.zk.delete(f'{STATUS_PATH}/{diedWorker}') # delete the status of died worker
                        logging.info("Task free: %s" % status)
            self.workers = workers
            self.distributeTask(event=event)
    
    def onClientChange(self, clients, event):
        if self.election.isMasterServer():
            # calculate died client
            diedClients = list(set(self.clients) - set(clients))
            if len(diedClients) > 0:
                # delete the result path of died clients
                for diedClient in diedClients:
                    self.zk.delete(f"{RESULTS_PATH}/{diedClient}", recursive=True)
                    logging.info("client %s died" % diedClient)
            self.clients = clients
            logging.info("clients are: %s" % clients)
                
if __name__ == '__main__':
    zk = server.init()
    master = Master(zk)
    while True:
        time.sleep(1)

