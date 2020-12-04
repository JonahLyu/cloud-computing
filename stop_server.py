import yaml
from fabric import Connection

print("Try to stop all master and worker servers...")
with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

hosts = [(k, v) for k, v in config['hosts'].items()]

instance_count = len(hosts)

from kazoo.client import KazooClient
cluster_ip = list(config['hosts'].keys())[0]

zk = KazooClient(hosts=f'{cluster_ip}:2181')

zk.start()

# let range run from 1 to n!
for idx in range(1, instance_count + 1):
    ip_pair = hosts[idx - 1]
    pub_ip = ip_pair[0]
    print (f'connecting to {pub_ip}')
    c = Connection(f'ubuntu@{pub_ip}', connect_kwargs={'key_filename': config['ssh_path']})
    c.sudo('''kill -9 $(sudo lsof -i:2181 | grep python | awk '{print $2}')''', warn=True, hide=True)

# clean the status of all workers
zk.delete("/status", recursive=True)
# clean all tasks
zk.delete("/tasks", recursive=True)
# clean all results
zk.delete("/results", recursive=True)
input('wait to quit')
zk.stop()