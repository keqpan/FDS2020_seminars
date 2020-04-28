import os
import numpy as np
import pandas as pd
import tarfile
import urllib.request
import zipfile
from glob import glob

data_dir = 'data'
raw_fname = 'nycflights.tar.gz'
dir_name = 'nycflights'
url = "https://storage.googleapis.com/dask-tutorial-data/nycflights.tar.gz"


def get_flights_data(data_dir, raw_fname, dir_name, url):
    flights_raw = os.path.join(data_dir, raw_fname)
    flightdir = os.path.join(data_dir, dir_name)
    jsondir = os.path.join(data_dir, 'flightjson')

    os.makedirs(data_dir, exist_ok=True)

    if not os.path.exists(flights_raw):
        print("- Downloading NYC Flights dataset... ", end='', flush=True)
        urllib.request.urlretrieve(url, flights_raw)
        print("done", flush=True)

    if not os.path.exists(flightdir):
        print("- Extracting flight data... ", end='', flush=True)
        tar_path = os.path.join(data_dir, raw_fname)
        with tarfile.open(tar_path, mode='r:gz') as flights:
            flights.extractall(data_dir)
        print("done", flush=True)
    
    n_rows = 10000  # Just take the first 10000 rows for the demo
    if not os.path.exists(jsondir):
        print("- Creating json data... ", end='', flush=True)
        os.mkdir(jsondir)
        for path in glob(os.path.join(data_dir, dir_name, '*.csv')):
            prefix = os.path.splitext(os.path.basename(path))[0]
            
            df = pd.read_csv(path).iloc[:n_rows]
            df.to_json(os.path.join(jsondir, prefix + '.json'),
                       orient='records', lines=True)
        print("done", flush=True)


def main(args):
    print("Setting up data directory")
    print("-------------------------")
    get_flights_data(data_dir, raw_fname, dir_name, url)
    print("** Finished! **")
    
    


if __name__ == '__main__':
    main()
