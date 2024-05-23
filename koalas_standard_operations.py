import databricks.koalas as ks
import dask
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import pandas as pd
import numpy as np
import time

print('pandas version: %s' % pd.__version__)
print('numpy version: %s' % np.__version__)
print('koalas version: %s' % ks.__version__)
print('dask version: %s' % dask.__version__)

url = "../yellow_tripdata_2011-01.parquet"
koalas_data = ks.read_parquet(url)

print("Number of rows:", len(koalas_data))
print()
print(koalas_data.head(20))

expr_filter = (koalas_data['tip_amount'] >= 1) & (koalas_data['tip_amount'] <= 5)
 
print(f'Filtered data is {len(koalas_data[expr_filter]) / len(koalas_data) * 100}% of total data')

def benchmark(f, df, benchmarks, name, **kwargs):
    """Benchmark the given function against the given DataFrame.

    Parameters
    ----------
    f: function to benchmark
    df: data frame
    benchmarks: container for benchmark results
    name: task name


    Returns
    -------
    Duration (in seconds) of the given operation
    """

    start_time = time.time()
    ret = f(df, **kwargs)
    benchmarks['duration'].append(time.time() - start_time)
    benchmarks['task'].append(name)
    print(f"{name} took: {benchmarks['duration'][-1]} seconds")
    return benchmarks['duration'][-1]


def get_results(benchmarks):
    """Return a pandas DataFrame containing benchmark results."""
    return pd.DataFrame.from_dict(benchmarks)

paths = ["../yellow_tripdata_201"+str(i)+"-01.parquet" for i in range(1,4)]

koalas_data = ks.read_parquet(paths[0])
koalas_benchmarks = {
    'duration': [],     # in seconds
    'task': []
}

sum_df = koalas_data.fare_amount + koalas_data.tip_amount
print(type(sum_df))
sum_df = sum_df.to_pandas()
print(type(sum_df))