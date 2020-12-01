import fabric
import jinja2
import yaml
import numpy as np


with open('config.yaml') as f:
    config = yaml.load(f)

# start watching
from kazoo.client import KazooClient
import logging

logging.basicConfig()

cluster_ip = list(config['hosts'].keys())[0]

zk = KazooClient(hosts=f'{cluster_ip}:2181')

zk.start()
zk.create("/app", b"start", ephemeral=True)

row = 30
zk.delete("/rowsQueue", recursive=True)
rowsQueue = zk.LockingQueue("/rowsQueue")
allRows = []
for i in range(row):
    allRows.append(str(i).encode("utf-8"))

print("deploy tasks: ", row)
rowsQueue.put_all(allRows)

# watch the server status
@zk.ChildrenWatch("/servers")
def watch_server(servers):
    print("servers are %s" % servers)

# watch the app data status
@zk.DataWatch("/app")
def watch_node(data, stat):
    if data is not None:
        print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

hosts = [(k, v) for k, v in config['hosts'].items()]

from fabric import Connection

instance_count = len(hosts)

# let range run from 1 to n!
for idx in range(1, instance_count + 1):
    ip_pair = hosts[idx - 1]
    pub_ip = ip_pair[0]
    print (f'connecting to {pub_ip}')
    c = Connection(f'ubuntu@{pub_ip}', connect_kwargs={'key_filename': config['ssh_path']})
    
    c.put(local="task.py", remote="/home/ubuntu/task.py")
    c.sudo(f'python /home/ubuntu/task.py 2>/dev/null >/dev/null &', warn=True)


input("wait to quit:\n")
zk.stop()