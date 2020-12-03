import yaml

MASTER_NUM_ON_EACH_NODE = 1
WORKER_NUM_ON_EACH_NODE = 2

with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

hosts = [(k, v) for k, v in config['hosts'].items()]

from fabric import Connection

instance_count = len(hosts)

from kazoo.client import KazooClient
cluster_ip = list(config['hosts'].keys())[0]

zk = KazooClient(hosts=f'{cluster_ip}:2181')

zk.start()

# watch the workers status
@zk.ChildrenWatch("/workers")
def watch_worker(workers):
    print("%d workers: %s" % (len(workers), workers))


# watch the master status
@zk.ChildrenWatch("/master")
def watch_master(masters):
    print("%d masters: %s" % (len(masters), masters))

# let range run from 1 to n!
for idx in range(1, instance_count + 1):
    ip_pair = hosts[idx - 1]
    pub_ip = ip_pair[0]
    print (f'connecting to {pub_ip}')
    c = Connection(f'ubuntu@{pub_ip}', connect_kwargs={'key_filename': config['ssh_path']})
    c.sudo('''kill -9 $(ps -ef | grep worker.py | grep root | awk '{ print $2}')''', warn=True)
    c.sudo('''kill -9 $(ps -ef | grep master.py | grep root | awk '{ print $2}')''', warn=True)

    c.run("mkdir -p /home/ubuntu/app")
    c.put(local="./app/election.py", remote="/home/ubuntu/app/election.py")
    c.put(local="./app/server.py", remote="/home/ubuntu/app/server.py")
    c.put(local="./app/worker.py", remote="/home/ubuntu/app/worker.py")
    c.put(local="./app/master.py", remote="/home/ubuntu/app/master.py")
    for i in range(MASTER_NUM_ON_EACH_NODE):
        c.sudo(f'python /home/ubuntu/app/master.py 2>master_error.log >master_out.log &', warn=True)
    for i in range(WORKER_NUM_ON_EACH_NODE):
        c.sudo(f'python /home/ubuntu/app/worker.py 2>worker_error.log >worker_out.log &', warn=True)


input('wait to quit')
zk.stop()