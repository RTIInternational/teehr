from dask_gateway import Gateway
gateway = Gateway("https://teehr-hub.rtiamanzi.org/services/dask-gateway", auth="jupyterhub")
gateway.list_clusters()
cluster = gateway.new_cluster()
cluster
gateway.stop_cluster(cluster_name=cluster.name)
