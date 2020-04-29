import os
from data_utils import fetch_data
from data import prepare_data
from utils import make_cluster
from dask.distributed import Client
from sklearn.ensemble import GradientBoostingRegressor
from dask_ml.xgboost import XGBRegressor
import pandas as pd

param_grid = {"learning_rate": [0.05, 0.1],
              "n_estimators": [20, 50, 100],
              "max_depth": [3]}
from benchmarking import ModelFitter, benchmark_models


def main(args):
    fetch_data(args.data_dir, args.dataset_name)
    
    X, y = prepare_data(os.path.join(args.data_dir, args.dataset_name, '*.csv'))

    print("Starting dask cluster ...")
    cluster = make_cluster(n_workers=args.n_workers)
    client = Client()
    
    # wrapping our target regressors for further benchmarking
    sk_xgb = ModelFitter(GradientBoostingRegressor(), True)
    dask_xgb = ModelFitter(XGBRegressor())

    print("Starting benchmarking process ...")
    results = benchmark_models(X, y, [sk_xgb, dask_xgb], args.n_folds, param_grid)
    
    res_df = pd.DataFrame(index=["sklearn", "dask"], data=results, columns=["GS time", "training time", "Metric"])
    print(res_df)


if __name__ == '__main__':
    main()
