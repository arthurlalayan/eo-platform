from dask_gateway import Gateway


def create_cluster(url, nodes_number):
    gateway = Gateway(url)
    cluster = gateway.new_cluster(shutdown_on_close=False)
    cluster.scale(nodes_number)
    return cluster
