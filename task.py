
from kazoo.client import KazooClient

import yaml,logging,time,random


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
    rowsQueue = zk.LockingQueue("/rowsQueue")
    while rowsQueue.__len__() != 0 :
        v = rowsQueue.get(timeout=3)
        if v is None:
            logger.info('Get task time out')
            break
        info = f'{myid} takes ' + v.decode("utf-8") 
        logger.info(info)
        zk.set("/app" , info.encode("utf-8"))
        time.sleep(float(random.randint(0,2)))
        rowsQueue.consume()
    if rowsQueue.__len__() == 0:
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