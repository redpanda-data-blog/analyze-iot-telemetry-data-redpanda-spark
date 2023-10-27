# iot-telemetry-spark-streaming

IoT Telemetry Processing with Apache Spark and RedPanda

## Prerequisites

* Docker Engine (version 24.0.6)
* Python Runtime (version 3.9)

## Steps to run

### Docker Deployment

Run the docker_compose.yaml under '/deployment' to launch Apache Spark and Redpanda

```
docker compose up -d
```

Copy and execute the PySoark program to process data

```
docker cp telemetry_processing.py spark-master:/home
docker exec spark-master pip install py4j
docker exec spark-master python /home/telemetry_processing.py
```


### IoT Device

Setup a separate Python 3 virtual environment and run the following commands under /iot to install dependencies and execute the IoT device simulation script

```
pip install kafka-python

python iot_device.py 
```

### Data Visualization

Setup a separate Python 3 virtual environment and run the following commands under /viz to install dependencies and execute the visualization program

```
pip install matplotlib

pip install kafka-python

python telemetry_viz.py
```

