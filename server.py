import sys, logging, signal
from kazoo.client import KazooClient

ELECTION_PATH="/master"
TASKS_PATH="/tasks"
PARAMS_PATH="/params"
WORKERS_PATH="/workers"
RESULTS_PATH="/results"
STATUS_PATH="/status"

def init():
    logging.basicConfig(format='%(asctime)s %(message)s',level=logging.ERROR)
    zk = KazooClient(hosts="127.0.0.1:2181")
    zk.start()
    zk.ensure_path(ELECTION_PATH)
    zk.ensure_path(TASKS_PATH)
    zk.ensure_path(PARAMS_PATH)
    zk.ensure_path(WORKERS_PATH)
    zk.ensure_path(RESULTS_PATH)
    zk.ensure_path(STATUS_PATH)

    # close the zk connection with Ctrl + c signal
    def interrupt_handler(signal, frame):
        zk.stop()
        sys.exit(0)
    # handle interrupt signal 
    signal.signal(signal.SIGINT, interrupt_handler)

    return zk