import yaml, time

MASTER_NUM_ON_EACH_NODE = 1
WORKER_NUM_ON_EACH_NODE = 3

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
    masterNum = len(masters)
    print("current master num: %d" % masterNum)

# kill all worker and master
print("kill the server...")
for idx in range(1, instance_count + 1):
    ip_pair = hosts[idx - 1]
    pub_ip = ip_pair[0]
    print (f'connecting to {pub_ip}')
    c = Connection(f'ubuntu@{pub_ip}', connect_kwargs={'key_filename': config['ssh_path']})
    c.sudo('''kill -9 $(sudo lsof -i:2181 | grep python | awk '{print $2}')''', warn=True, hide=True)

while masterNum > 1:
    time.sleep(1)

# launch the server
print("launch the server...")
for idx in range(1, instance_count + 1):
    ip_pair = hosts[idx - 1]
    pub_ip = ip_pair[0]
    print (f'connecting to {pub_ip}')
    c = Connection(f'ubuntu@{pub_ip}', connect_kwargs={'key_filename': config['ssh_path']})
    c.run("mkdir -p /home/ubuntu/app")
    c.put(local="./app/election.py", remote="/home/ubuntu/app/election.py")
    c.put(local="./app/server.py", remote="/home/ubuntu/app/server.py")
    c.put(local="./app/worker.py", remote="/home/ubuntu/app/worker.py")
    c.put(local="./app/master.py", remote="/home/ubuntu/app/master.py")
    for i in range(MASTER_NUM_ON_EACH_NODE):
        c.sudo(f'python /home/ubuntu/app/master.py 2>/home/ubuntu/app/master_error.log >/home/ubuntu/app/master_out.log &', warn=True, asynchronous=True)
    for i in range(WORKER_NUM_ON_EACH_NODE):
        c.sudo(f'python /home/ubuntu/app/worker.py 2>/home/ubuntu/app/worker_error.log >/home/ubuntu/app/worker_out.log &', warn=True, asynchronous=True)


input('wait to quit')
zk.stop()