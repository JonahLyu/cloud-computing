import yaml, time, sys

MASTER_NUM_ON_EACH_NODE = 1
WORKER_NUM = 9

# check argument
if len(sys.argv) < 2:
    print("Usage: python start_server.py master/worker")
    sys.exit(0)
elif sys.argv[1] == 'master':
    print(f"Try to launch {MASTER_NUM_ON_EACH_NODE} master(s) on each node")
elif sys.argv[1] == 'worker':
    print(f"Try to launch {WORKER_NUM} worker(s)")
else:
    print("Usage: python start_server.py master/worker")
    sys.exit(0)

with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

hosts = [(k, v) for k, v in config['hosts'].items()]

from fabric import Connection

instance_count = len(hosts)

from kazoo.client import KazooClient
cluster_ip = list(config['hosts'].keys())[0]

zk = KazooClient(hosts=f'{cluster_ip}:2181')

zk.start()

masterNum = 0
workerNum = 0
# watch the workers status
@zk.ChildrenWatch("/workers")
def watch_worker(workers):
    workerNum = len(workers)
    print("current worker num: %d" % workerNum)


# watch the master status
@zk.ChildrenWatch("/master")
def watch_master(masters):
    masterNum = len(masters) - 1 # ignore child /current_master
    print("current master num: %d" % masterNum)

# kill all worker and master
print(f"kill all old {sys.argv[1]} servers...")
for idx in range(1, instance_count + 1):
    ip_pair = hosts[idx - 1]
    pub_ip = ip_pair[0]
    print (f'connecting to {pub_ip}')
    c = Connection(f'ubuntu@{pub_ip}', connect_kwargs={'key_filename': config['ssh_path']})
    if sys.argv[1] == "worker":
        c.sudo('''kill -9 $( ps -ef | grep 'server.py worker' | awk '{print $2}')''', warn=True, hide=True)
    if sys.argv[1] == "master":
        c.sudo('''kill -9 $( ps -ef | grep 'server.py master' | awk '{print $2}')''', warn=True, hide=True)

if sys.argv[1] == "master":
    while masterNum > 1:
        time.sleep(1)

# launch the server
print(f"launch new {sys.argv[1]} server...")
for idx in range(1, instance_count + 1):
    ip_pair = hosts[idx - 1]
    pub_ip = ip_pair[0]
    print (f'connecting to {pub_ip}')
    c = Connection(f'ubuntu@{pub_ip}', connect_kwargs={'key_filename': config['ssh_path']})
    c.run("mkdir -p /home/ubuntu/app")
    c.run("mkdir -p /home/ubuntu/app/log")
    c.put(local="./app/utils.py", remote="/home/ubuntu/app/utils.py")
    c.put(local="./app/server.py", remote="/home/ubuntu/app/server.py")
    
    if sys.argv[1] == "master":
        c.put(local="./app/election.py", remote="/home/ubuntu/app/election.py")
        c.put(local="./app/master.py", remote="/home/ubuntu/app/master.py")
        for i in range(MASTER_NUM_ON_EACH_NODE):
            c.sudo(f'python /home/ubuntu/app/server.py master 2>/home/ubuntu/app/log/master_{i}.log >/dev/null &', warn=True, asynchronous=True)
    if sys.argv[1] == "worker":
        c.put(local="./app/worker.py", remote="/home/ubuntu/app/worker.py")
        # deploy worker uniformly on all nodes
        m = WORKER_NUM % instance_count
        n = WORKER_NUM // instance_count
        if m == 0:
            WORKER_NUM_ON_EACH_NODE = n
        elif idx <= m:
            WORKER_NUM_ON_EACH_NODE = n + 1
        else:
            WORKER_NUM_ON_EACH_NODE = n
        for i in range(WORKER_NUM_ON_EACH_NODE):
            c.sudo(f'python /home/ubuntu/app/server.py worker 2>/home/ubuntu/app/log/worker_{i}.log >/dev/null &', warn=True, asynchronous=True)


input('wait to quit')
zk.stop()