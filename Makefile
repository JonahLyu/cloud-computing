.PHONY: start master worker client election
start:
	python start_server.py

stop:
	python stop_server.py

master:
	python ./app/master.py

worker:
	python ./app/worker.py

client:
	python ./client.py

election:
	python ./app/election.py