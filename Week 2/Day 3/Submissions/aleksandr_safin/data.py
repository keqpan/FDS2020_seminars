import dask.dataframe as dd
import numpy as np


def prepare_data(data_glob_path, target_columns = ["DepDelay"]):
    df = dd.read_csv(data_glob_path, dtype={"TailNum": object, "CRSElapsedTime": np.float64}) # this is necessary to read data correctly
    
    # drop the columns with a large share of nan's
    df = df.drop(columns=["UniqueCarrier", "TailNum", "Origin", "Dest"], axis=0)
    df = df.dropna()
    
    # preparing target column and input dataframe
    df_target = df[target_columns]
    df = df.drop(columns=target_columns, axis=0)
    
    # converting to dask_array to avoid problems with corrupted index and further related bugs
    X = df.to_dask_array(lengths=True)
    y = df_target.to_dask_array(lengths=True)
    return X, y