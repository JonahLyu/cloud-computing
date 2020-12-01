from kazoo.client import KazooClient

import yaml

import logging

logging.basicConfig()

with open('config.yaml') as f:
    config = yaml.load(f)

cluster_ip = list(config['hosts'].keys())[0]

zk = KazooClient(hosts=f'{cluster_ip}:2181')

zk.start()

# zk.create('/app', b"this is test" ,ephemeral=True, makepath=True)

@zk.DataWatch("/app/row")
def watch_node(data, stat):
    if data is not None:
        print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))


input("wait to quit:\n")
