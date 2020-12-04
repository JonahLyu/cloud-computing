.PHONY: start master worker client election init query

# start the app server in zk cluster
start:
	python start_server.py

# stop the app server in zk cluster
stop:
	python stop_server.py

# start a client
client:
	python ./client.py

# initilize the zk cluster
init:
	python ./init_zk_cluster.py

# query the zk cluster
query:
	python ./query_zk_cluster.py

# start a master on current node, this is for test
master:
	python ./app/master.py

# start a worker on current node, this is for test
worker:
	python ./app/worker.py

# add a new election candidate from current node, this is for test
election:
	python ./app/election.py