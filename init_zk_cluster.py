import fabric
import jinja2
import yaml

jinja_environment = jinja2.Environment(loader=jinja2.FileSystemLoader('etc'))
template = jinja_environment.get_template('zoo.cfg.template')

with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

hosts = [(k, v) for k, v in config['hosts'].items()]

from fabric import Connection

instance_count = len(hosts)

# let range run from 1 to n!
for idx in range(1, instance_count + 1):

    ip_pair = hosts[idx - 1]
    pub_ip = ip_pair[0]

    c = Connection(f'ubuntu@{pub_ip}', connect_kwargs={'key_filename': config['ssh_path']})
    

    # with c.cd('/home/ubuntu'):
    #     c.run('wget https://apache.mirrors.nublue.co.uk/zookeeper/zookeeper-3.6.2/apache-zookeeper-3.6.2-bin.tar.gz')
    #     c.run('tar -xzf apache-zookeeper-3.6.2-bin.tar.gz')

    # c.sudo('mv /home/ubuntu/apache-zookeeper-3.6.2-bin /usr/local/zookeeper', warn=True)
    
    c.sudo('rm -rf /var/lib/zookeeper')
    c.sudo('mkdir -p /var/lib/zookeeper')

    file_name = f'etc/zoo_{idx}.cfg'
    with open(file_name, 'w') as cfg_file:

        line_template = 'server.{idx}={priv_ip}:2888:3888'

        lines = []
        # let i run from 1 to n
        for i in range(1, instance_count + 1):
            priv_ip = hosts[i - 1][1]
            lines.append(line_template.format(idx=i, priv_ip=priv_ip if i is not idx else '0.0.0.0'))

        server_list = "\n".join(lines)

        cfg_file.write(template.render(server_list=server_list))

    c.put(local=file_name, remote="/home/ubuntu/zoo.cfg")
    c.sudo('cp /home/ubuntu/zoo.cfg /usr/local/zookeeper/conf/zoo.cfg')
    c.sudo(f'echo {idx} | sudo tee -a /var/lib/zookeeper/myid')
    c.sudo(f'echo {pub_ip} | sudo tee -a /var/lib/zookeeper/host')

    # c.sudo(f'apt-get update')
    # c.sudo(f'apt-get install python-is-python3 python3-pip -y')
    # c.sudo(f'pip3 install kazoo')
    # c.sudo(f'pip3 install numpy matplotlib')
    # c.sudo(f'apt install -y openjdk-11-jdk')

    c.run('export JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64')
    c.sudo('''kill -9 $(lsof -i:2181 | awk '{print $2}')''', warn=True, hide=True)
    c.sudo('/usr/local/zookeeper/bin/zkServer.sh start')
