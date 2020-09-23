import pickle
from gzip import GzipFile
from itertools import islice

def batchiter(it, batch_size):
    it = iter(it)
    while True:
        batch = list(islice(it, batch_size))
        if batch:
            yield batch
        else:
            break

def decode(path):
    "decodes a gzip+pickle compressed object from a file path"
    with GzipFile(path) as f:
        obj = pickle.load(f)
        return str(obj)
