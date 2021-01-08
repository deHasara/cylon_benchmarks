import time
import numpy as np
import pandas as pd
import pyarrow as pa
from pycylon import Table
from pycylon import CylonContext


ctx: CylonContext = CylonContext(config=None, distributed=False)
num_rows = 1000000 #10_000_000
data = np.random.randn(num_rows)

df = pd.DataFrame({'data{}'.format(i): data
                   for i in range(100)})

df['key'] = np.random.randint(0, 100, size=num_rows)

ct = Table.from_pandas(ctx, df)
ct.set_index(range(0, num_rows))

def compare_time(op):
    if op == 'inequality':
        t1 = time.time()
        new_df = df['data0'] != 2
        # print(new_df)
        t2 = time.time()

        # print(ct)
        t3 = time.time()
        new_ct = ct['data0'] != 2
        # print(new_ct)
        t4 = time.time()
    elif op == 'equality':
        t1 = time.time()
        new_df = df['data0'] == 2
        # print(new_df)
        t2 = time.time()

        # print(ct)
        t3 = time.time()
        new_ct = ct['data0'] == 2
        # print(new_ct)
        t4 = time.time()
    elif op == 'greater than':
        t1 = time.time()
        new_df = df['data0'] > 2
        # print(new_df)
        t2 = time.time()

        # print(ct)
        t3 = time.time()
        new_ct = ct['data0'] > 2
        # print(new_ct)
        t4 = time.time()
    elif op == 'less than':
        t1 = time.time()
        new_df = df['data0'] < 2
        # print(new_df)
        t2 = time.time()

        # print(ct)
        t3 = time.time()
        new_ct = ct['data0'] < 2
        # print(new_ct)
        t4 = time.time()
    return 'pandas :' , t2 - t1, 'cylon :', t4 - t3




print('inequality :',compare_time('inequality'))
print('equality :',compare_time('equality'))
print('greater than :',compare_time('greater than'))
print('less than :',compare_time('less than'))















