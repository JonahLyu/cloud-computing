# CW20-20
Lyu Jonah (jl17031)

# Configure Instances

## Prerequisites

- [ ] python >3.6
- [ ] AWS ssh key configured
- [ ] edit `config.yaml` to declare nodes Public/Privat IP

### Setup Python Dependencies
- [ ] run `pip install -r requirements.txt`

# Mandelbrot Set APP guide

**Launch the client**

The servers have been deployed already, so the quickest way to learn about what the app does is launching a client:

```
make client
```

example input

```
enter number of slices to deploy: 9
enter zoom (float value): 200
```
![result_zoom_200}](/png/result_zoom_2000.0.png)


**Launch the master and worker server**

The following scripts will stop all old servers and redeploy them

```
python start_server.py master
python start_server.py worker
```

edit following lines in `start_server.py` to scale up/down:

```python
MASTER_NUM_ON_EACH_NODE = 1
WORKER_NUM = 6   # deploy uniformly on all nodes
```

You can also manually launch a master or worker without stopping the existing servers by following command:

```shell
make master
make worker
```


**Stop the server**

This will kill both master and worker servers

```
make stop
```