
from kazoo.client import KazooClient

import yaml,logging,time,random
import numpy as np

def mandelbrot_set(width, height, startRow, endRow, zoom=1, x_off=0, y_off=0, niter=256):
    w,h = width, height
    pixels = np.arange(w*(endRow - startRow), dtype=np.uint16).reshape(endRow - startRow, w)

    for x in range(w): 
        zx = 1.5*(x + x_off - 3*w/4)/(0.5*zoom*w)
        for y in range(startRow, endRow):
            
            zy = 1.0*(y + y_off - h/2)/(0.5*zoom*h)
            
            z = complex(zx, zy)
            c = complex(0, 0)
            
            for i in range(niter):
                if abs(c) > 4: break
                c = c**2 + z

            color = (i << 21) + (i << 10)  + i * 8
            pixels[y - startRow][x] = color
    return pixels

def register():
    global myid, host
    with open("/var/lib/zookeeper/myid", "r") as f:
        myid = f.read().rstrip('\n')
    with open("/var/lib/zookeeper/host", "r") as f:
        host = f.read().rstrip('\n')
    logger.info('Register server %s with host: %s', myid, host)
    zk.create(f'/servers/server-{myid}', host.encode("utf-8"), ephemeral=True, makepath=True)

def process():
    global myid, host
    taskQueue = zk.LockingQueue("/taskQueue")
    # while taskQueue.__len__() != 0 :
        # v = taskQueue.get(timeout=3)
    while True :
        v = taskQueue.get()
        if v is None:
            logger.info('Get task time out')
            break
        task = v.decode("utf-8")
        logger.info(f'{myid} get task: {task}')
        params = task.split(':')
        width, height, zoom = int(params[0]),int(params[1]),float(params[2])
        startRow, endRow = int(params[3]), int(params[4])
        pixels = mandelbrot_set(width, height, startRow, endRow, zoom)
        zk.create(f"/data/{startRow}:{endRow}", pixels.tobytes(), makepath=True)
        taskQueue.consume()
    if taskQueue.__len__() == 0:
        logger.info('Task Queue is now empty')
    else:
        logger.info('Unexpected break from while loop')
    logger.info('End process')

if __name__ == "__main__":
    # initialize the logger
    logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger('app')

    # initialize
    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()

    myid = ''
    host = ''

    register()
    process()

    # stop zk connection
    zk.stop()