import pandas as pd
import numpy as np
from hdf5_getters import *
from utils import get_all_files
import sys


def reducer():
    with open("ge1.csv", "w") as f:
        for line in sys.stdin:
            line = line.strip()
            if line=="":
                continue
            f.write(line + "\n")


if __name__ == '__main__':
    reducer()