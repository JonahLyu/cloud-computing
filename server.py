import sys, logging, signal
from kazoo.client import KazooClient

ELECTION_PATH="/master"
TASKS_PATH="/tasks"
DATA_PATH="/data"
WORKERS_PATH="/workers"

def init():
    logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)
    zk = KazooClient(hosts="127.0.0.1:2181")
    zk.start()
    zk.ensure_path(ELECTION_PATH)
    zk.ensure_path(TASKS_PATH)
    zk.ensure_path(DATA_PATH)
    zk.ensure_path(WORKERS_PATH)

    # close the zk connection with Ctrl + c signal
    def interrupt_handler(signal, frame):
        zk.stop()
        sys.exit(0)
    # handle interrupt signal 
    signal.signal(signal.SIGINT, interrupt_handler)

    return zk