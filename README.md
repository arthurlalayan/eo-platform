# Distributed EO Data Processing using Dask

This GitHub package provides a platform for processing Earth Observation (EO) data in a distributed manner using Dask. The platform leverages Dask, a parallel computing library, to scale data processing tasks across a cluster of machines.

## Features

- Distributed processing of EO data using Dask.
- Kubernetes deployment using Helm for easy setup and scaling.
- Customizable cluster options for worker configuration.


## Building
Clone the Repository: Start by cloning the repository containing the Dockerfile for building the package.
```bash
git clone https://github.com/arthurlalayan/eo-platform.git
cd eo-platform
```

Build the Docker Image: In the repository's root directory (where the [Dockerfile](/Dockerfile) is located), use the docker build command to build the Docker image.

```bash
docker build -t eo-platform:<tag> .
```


## Prerequisites for Deployment

- Kubernetes cluster with Helm installed.

##  Dask Gateway Deployment

To deploy the Dask Gateway in Kubernetes, run the following commands:

1. Add the Dask Helm repository:
helm repo add dask https://helm.dask.org
2. Update the Helm repositories:
helm repo update
3. Install the Dask Gateway:
helm install --version 2022.11.0 gateway dask/dask-gateway --set-file gateway.extraConfig.clusteroptions=config.py

## Customizing Cluster Options

The cluster options, such as the number of worker cores, worker memory, container image, and environment variables, can be customized to suit your specific requirements. The configuration is specified in the `config.py` file.

Here's an example of the `config.py` file:

```
from dask_gateway_server.options import Options, Integer, Float, String, Mapping

def option_handler(options):
 return {
     "worker_cores": options.worker_cores,
     "worker_memory": "%fG" % options.worker_memory,
     "image": options.image,
     "environment": options.environment
 }

c.Backend.cluster_options = Options(
 Integer("worker_cores", 2, min=2, max=2, label="Worker Cores"),
 Float("worker_memory", 4, min=4, max=4, label="Worker Memory (GiB)"),
 String("image", default="arthurlalayan/eo_dask_gateway:0.0.5", label="Image"),
 Mapping("environment", default=dict(), label="Environment"),
 handler=option_handler,
)
```

In this example, the cluster is configured with 2 worker cores and 4GB of worker memory. You can modify these values as needed.

## Platform Deployment

To deploy the platform in Kubernetes, follow these steps:
1. Configure IP and port of the service in [kube.yaml](config/kube.yaml)
2. Deploy: kubectl apply -f kube.yaml


## Usage

See the example of platform usage and visualization in [example.ipynb](usage/example.ipynb), which is Jupyter Notebook based andSee the example of platform usage and visualization in example.ipynb.

The example.ipynb Jupyter Notebook provides a demonstration of how to use the platform for processing and visualizing EO data using Dask's distributed computing capabilities provides a demonstration of how to use the platform for processing and visualizing EO data using Dask's distributed computing capabilities
