import argparse
from main import main

def set_args(parser):
    parser.add_argument('--data_dir', default="data", type=str,
                        help="data directory")
    parser.add_argument('--dataset_name', default="nycflights", type=str,
                        help="name of the dataset")
    parser.add_argument('--n_workers', default=10, type=int,
                        help="number of workers in cluster")
    parser.add_argument('--n_folds', type=int,
                        default=3, help='number of folds for CV')
    parser.add_argument('--random_state', type=int,
                        default=42, help='random state seed')
    return parser


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    set_args(parser)
    args = parser.parse_args()
    
    main(args)