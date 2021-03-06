import fabric
import jinja2
import yaml

with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

hosts = [(k, v) for k, v in config['hosts'].items()]

from fabric import Connection

instance_count = len(hosts)

# let range run from 1 to n!
for idx in range(1, instance_count + 1):
    print(f'---- {idx} ---')
    ip_pair = hosts[idx - 1]
    pub_ip = ip_pair[0]
    print (f'connecting to {pub_ip}')
    c = Connection(f'ubuntu@{pub_ip}', connect_kwargs={'key_filename': config['ssh_path']})
    
    # c.sudo('/usr/local/zookeeper/bin/zkServer.sh status', warn=True)
    # c.sudo('cat /home/ubuntu/app/log/master_*.log', warn=True)
    c.sudo('cat /home/ubuntu/app/log/worker_*.log', warn=True)
