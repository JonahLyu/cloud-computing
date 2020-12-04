import utils, sys, logging, time
from master import Master
from worker import Worker

if __name__ == '__main__':

    # get 127.0.0.1:2181 local zk client
    zk = utils.init('local')

    # this is a master cadidate
    if (sys.argv[1]) == 'master':
        master = Master(zk)

    # this is a worker
    elif (sys.argv[1]) == 'worker':
        worker = Worker(zk)

    # this is nothing
    else:
        logging.info("wrong arg")
        sys.exit(0)

    while True:
        time.sleep(1)