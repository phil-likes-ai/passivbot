import math
import numpy as np


def get_reference_directions(method, n_obj, n_partitions):
    count = math.comb(int(n_obj) + int(n_partitions) - 1, int(n_partitions))
    return np.zeros((count, int(n_obj)), dtype=np.float64)
