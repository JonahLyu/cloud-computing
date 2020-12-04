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

**Launch the server**
```
make start
```

```python
MASTER_NUM_ON_EACH_NODE = 1
WORKER_NUM_ON_EACH_NODE = 3
```


**Launch the client**
```
make client
```

example input
```
enter number of slices to deploy: 9
enter zoom (float value): 200
```
![result_zoom_200}](/png/result_zoom_2000.0.png)

**Stop the server**
```
make stop
```