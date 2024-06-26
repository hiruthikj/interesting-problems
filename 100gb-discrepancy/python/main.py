####### Code in https://github.com/hiruthikj/interesting-problems/blob/main/100gb-discrepancy/python/main.py ########

# Using KV store available in standard library of python 
# Advantage: Has API like python dict, is faster than sqlite3 (https://remusao.github.io/posts/python-dbm-module.html)
# Not platform independent (https://docs.python.org/3/library/dbm.html) - can dockerize
import dbm

# Using dask as a simple way to read large CSV parallely and in chunks (will fit in memory)
from dask import dataframe as dd
from dask.base import tokenize, normalize_token
import pandas as pd

# TODO: add slug, and cleanup. Can use tempfile module
db_filename = "kv_store"
output_filename = "output.txt"
 

class Point:
    """
    Have to use this way for hashing to work with dask library
    https://docs.dask.org/en/latest/custom-collections.html#implementing-deterministic-hashing
    """
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __dask_tokenize__(self):
        return normalize_token(Point), self.x, self.y


def hash_point(x: str, y: str) -> str:
    """Function to hash x and y values of a point """
    hash_val = tokenize(Point(x, y))
    return hash_val


def add_point_to_kv_store(row):
    """Adds each x,y encountered in source file to kv store"""
    # count=0
    with dbm.open(db_filename, flag='c') as db:
        x, y = row['x'], row['y']
        key = hash_point(x, y)
        count = int(db.get(key, 0))
        count += 1
        db[key] = str(count)

    # with dbm.open(db_filename, flag='r') as db:
    #     print("fetched" ,db[key])


def remove_point_from_kv_store(row):
    """Remove each x,y encountered in mangled file to populated kv store"""
    with dbm.open(db_filename, flag='w') as db:
        x, y = row['x'], row['y']
        key = hash_point(x, y)
        count = int(db.get(key, 0))
        if count == 0:
            print(f"Modified one: {row['x']}, {row['y']}")
            return
        count -= 1
        db[key] = str(count)


def main():
    dtypes = {'x': 'str', 'y': 'str'}
    col_names = ["x", "y"]
    meta_df = pd.DataFrame({'x': [1], 'y': [2]}, columns=col_names)

    original_file_df = dd.read_csv('./sample_data/source2.txt', names=col_names, dtype=dtypes)
    original_file_df.apply(add_point_to_kv_store, axis=1, meta=meta_df).compute()

    print("\n")

    mangled_file_df = dd.read_csv('./sample_data/mangled2.txt', names=col_names, dtype=dtypes)
    mangled_file_df.apply(remove_point_from_kv_store, axis=1, meta=meta_df).compute()

    with dbm.open(db_filename, flag='r') as db:
        print(f"Using {dbm.whichdb(db_filename)}")
        # for key in db:
            # print(db[key])



if __name__ == "__main__":
    main()