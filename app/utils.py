from kazoo.client import KazooClient
import signal,sys,logging
import yaml

ELECTION_PATH="/master"
TASKS_PATH="/tasks"
PARAMS_PATH="/params"
WORKERS_PATH="/workers"
RESULTS_PATH="/results"
STATUS_PATH="/status"
CLIENT_PATH="/clients"

def init(path='local'):
    logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)
    if path == 'local':
        zk = KazooClient(hosts="127.0.0.1:2181")
    else: 
        with open('config.yaml') as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        cluster_ip = list(config['hosts'].keys())[0]
        print(cluster_ip)
        zk = KazooClient(hosts=f'{cluster_ip}:2181')
    zk.start()
    zk.ensure_path(ELECTION_PATH)
    zk.ensure_path(TASKS_PATH)
    zk.ensure_path(PARAMS_PATH)
    zk.ensure_path(WORKERS_PATH)
    zk.ensure_path(RESULTS_PATH)
    zk.ensure_path(STATUS_PATH)
    zk.ensure_path(CLIENT_PATH)

    # close the zk connection with Ctrl + c signal
    def interruptHandler(signal, frame):
        zk.stop()
        sys.exit(0)
    # handle interrupt signal 
    signal.signal(signal.SIGINT, interruptHandler)
    return zk