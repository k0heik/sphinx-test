import psutil
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster


def get_dask_client():
    n_cores = psutil.cpu_count()
    mem = psutil.virtual_memory()
    memory_limit_per_worker = mem.available / 1024 * 1024  # MB
    memory_limit_per_worker = \
        int(round(memory_limit_per_worker * 0.9 / n_cores))

    cluster = LocalCluster(
        n_workers=n_cores,
        threads_per_worker=1,
        memory_limit=f'{memory_limit_per_worker}MB'
    )
    client = Client(cluster)
    return client, cluster


def merge(df_left, df_right, how, on):
    return dd.merge(df_left, df_right, how=how, on=on)


def dask_dataframe_from_pandas(df, npartitions):
    return dd.from_pandas(df, npartitions=npartitions)


def make_meta(dtypes):
    return dd.utils.make_meta(dtypes)
