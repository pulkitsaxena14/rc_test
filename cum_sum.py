import numpy as np

def cum_sum(a, b):
    cu_a = np.cumsum(a)
    cu_b = np.cumsum(b)
    c = []
    for i in range(0,len(a)):
        l = []
        for j in range(0,len(cu_b)):
            if i == 0:
                cu_a_val = 0
            else:
                cu_a_val = cu_a[i-1]
            if j == 0:
                cu_b_val = 0
            else:
                cu_b_val = cu_b[j-1]
            val = np.maximum(0, (np.minimum(cu_a[i], cu_b[j]) - np.maximum(cu_a_val, cu_b_val)))
            if val != 0:
                l.append(val)
            if np.minimum(cu_a[i], cu_b[j]) == cu_a[i]:
                break
        c.append(l)
    return c


def test_cum_sum():
    a = [3600, 2400, 1200, 500]
    b = [1000, 1500, 700, 600, 300, 400, 100, 750, 800, 150, 500, 2000]
    c = [[1000, 1500, 700, 400], [200, 300, 400, 100, 750, 650], [150, 150, 500, 400], [500]]
    assert cum_sum(a, b) == c