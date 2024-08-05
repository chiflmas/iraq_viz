import logging
import mgrs
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from dask.distributed import Client
import iraq_viz as f
from tqdm import tqdm
import time

if __name__ == '__main__':
    # Load of csv file with iraq_sigacts data
    df = f.dataframe_format('iraq_sigacts.csv')
    # MGRS to Lat Lon transformation
    df['lat'], df['lon'] = zip(*df['MGRS'].apply(lambda x: f.mgrs_to_latlon(x)))
    # Delete rows with wrong MGRS coordinates
    df = df.dropna()
    df_sc = df.copy()

    # Dask Client with 6 workers
    client = Client(n_workers=6)

    # Convert df to a Dask dataframe with 500 partitions
    ddata = dd.from_pandas(df,
                           npartitions=500)

    start_time = time.time()
    print('Executing Dask with 6 workers and 12 threads')
    with ProgressBar():
        # Apply sun function with dask parallel processing and compute the dask object to get a pandas df.
        df[['Sunrise', 'Sunset', 'Daylight']] = ddata.apply(f.sun,
                                                            axis=1,
                                                            result_type="expand",
                                                            meta={0: 'datetime64[ns]',
                                                                  1: 'datetime64[ns]',
                                                                  2: 'int64'}).compute(scheduler='processes')

    client.shutdown()

    end_time = time.time()

    elapsed_time = end_time - start_time
    print('Total Dask time:{} s\n'.format(elapsed_time))
    start_time_sp = time.time()

    print('Executing Pandas apply\n')
    tqdm.pandas()
    df_sc[['Sunrise', 'Sunset', 'Daylight']] = df_sc.progress_apply(lambda x: f.sun(x),
                                                                    axis=1)
    end_time_sp = time.time()

    elapsed_time_sp = end_time_sp - start_time_sp
    print('Total Dask time:{} s'.format(elapsed_time_sp))

    f.create_bar_chart(elapsed_time, elapsed_time_sp)
    df.to_csv('iraq_viz.csv',
              index=False)



