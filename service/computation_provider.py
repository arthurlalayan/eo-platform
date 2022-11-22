from dask_gateway import Gateway


def create_cluster(url, nodes_number):
    gateway = Gateway(url)
    options = gateway.cluster_options()
    options.environment = dict(
        # DASK_DISTRIBUTED__SCHEDULER__WORKER_SATURATION="8",
        MALLOC_TRIM_THRESHOLD_="65536"
    )
    cluster = gateway.new_cluster(shutdown_on_close=False)
    cluster.scale(nodes_number)
    return cluster
