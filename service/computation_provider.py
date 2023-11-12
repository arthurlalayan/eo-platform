import time

from dask_gateway import Gateway
from dask_gateway.options import Options, Integer, Float, String, Mapping

from model.request_data_dto import RequestDataDto


def create_cluster(data: RequestDataDto):
    gateway = Gateway(data.computationURL)
    options = Options(
        Integer("worker_cores", data.cores, min=1, max=64, label="Worker Cores"),
        Float("worker_memory", data.ram, min=2, max=128, label="Worker Memory (GiB)"),
        String("image", default="arthurlalayan/eo_dask_gateway:0.0.5", label="Image"),
        Mapping("environment", default=dict(), label="Environment")
    )
    # options.environment = dict(
    #     DASK_DISTRIBUTED__SCHEDULER__WORKER_SATURATION="1",
    #     MALLOC_TRIM_THRESHOLD_="65536"
    # )
    cluster = gateway.new_cluster(options, shutdown_on_close=False)
    cluster.scale(data.nodes)

    client = cluster.get_client()

    start_time = time.time()
    while len(client.scheduler_info()["workers"]) < data.nodes:
        if (time.time() - start_time) < 10:
            break

    def install():
        import os
        os.system("pip install numba")

    client.run(install)

    return cluster
