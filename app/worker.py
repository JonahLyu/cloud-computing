import time, socket, os, uuid, sys, kazoo, logging, signal
import logging, utils
import numpy as np
ELECTION_PATH="/master"
TASKS_PATH="/tasks"
PARAMS_PATH="/params"
WORKERS_PATH="/workers"
STATUS_PATH="/status"
RESULTS_PATH="/results"


def mandelbrotSet(width, height, startRow, endRow, zoom=1, niter=256):
    w,h = width, height
    pixels = np.arange(w*(endRow - startRow), dtype=np.uint16).reshape(endRow - startRow, w)

    zoom = 1 / zoom
    for x in range(w): 
        for y in range(startRow, endRow):
            zx = (-1.0 + 2.0 * x / w) * w / h
            zy = -1.0 + 2.0 * y / h
            
            c = complex(-0.05 + zx * zoom, 0.6805 + zy * zoom)
            z = complex(0, 0)
            
            for i in range(niter):
                if abs(z) > 2: 
                    break
                z = z**2 + c

            color = (i << 21) + (i << 10)  + i * 8
            pixels[y - startRow,x] = color
    return pixels

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
        logging.info("Worker %s  created!" %(self.workerPath))
        #3.watch znode
        zk.DataWatch(self.statusPath, self.onAssignChange)   
    
    # do something upon the change on assignment
    def onAssignChange(self, taskID, stat):
        if taskID is not None and taskID.decode("utf-8") != "non":
            taskID = taskID.decode("utf-8")
            logging.info("Worker recieved task %s" % taskID) 
            #4.5. get the parameters of the task
            paramPath = f'{PARAMS_PATH}/{taskID}'
            taskPath = f'{TASKS_PATH}/{taskID}'
            clientID, _ = self.zk.get(taskPath)
            clientID = clientID.decode("utf-8").split('#')[1]
            logging.info(clientID)
            if self.zk.exists(paramPath) :
                data, _ = self.zk.get(paramPath)
                #6. execute task with data
                params = data.decode("utf-8").split(':')
                width, height, zoom = int(params[0]),int(params[1]),float(params[2])
                startRow, endRow = int(params[3]), int(params[4])
                pixels = mandelbrotSet(width, height, startRow, endRow, zoom)
                taskPath = f'{TASKS_PATH}/{taskID}'
                resultPath = f'{RESULTS_PATH}/{clientID}/{taskID}'
                # write result back to task
                if self.zk.exists(taskPath):
                    self.zk.create(resultPath, pixels.tobytes())
                    self.zk.set(taskPath, b"complete")
                    #free this worker
                    self.zk.set(self.statusPath, b"non")
                    logging.info("Worker completed task %s" % taskID)
                else :
                    #free this worker
                    self.zk.set(self.statusPath, b"non")
                    logging.info("Task %s not found, maybe the connection  was lost" %(resultPath))




if __name__ == '__main__':
    zk = utils.init('out')   #get out cluster zk client
    worker = Worker(zk)
    while True:
        time.sleep(1)