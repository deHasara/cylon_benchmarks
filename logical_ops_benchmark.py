import time
import numpy as np
import pandas as pd
import pyarrow as pa
from pycylon import Table
from pycylon import CylonContext


ctx: CylonContext = CylonContext(config=None, distributed=False)
num_rows = 10 #10_000_000
data = np.random.randn(num_rows)

df = pd.DataFrame({'data{}'.format(i): data
                   for i in range(100)})

df['key'] = np.random.randint(0, 100, size=num_rows)

ct = Table.from_pandas(ctx, df)
ct.set_index(range(0, num_rows))

def compare_time(op):
    if op == 'AND':
        new_df1 = df != 2
        new_df2 = df != 3
        t1 = time.time()
        new_df = new_df1 & new_df2
        #print(new_df)
        t2 = time.time()

        new_ct1 = ct != 2
        new_ct2 = ct != 3
        t3 = time.time()
        new_ct = new_ct1 & new_ct2
        #print(new_ct)
        t4 = time.time()
    elif op == 'OR':
        new_df1 = df != 2
        new_df2 = df != 3
        t1 = time.time()
        new_df = new_df1 | new_df2
        #print(new_df)
        t2 = time.time()

        new_ct1 = ct != 2
        new_ct2 = ct != 3
        t3 = time.time()
        new_ct = new_ct1 | new_ct2
        #print(new_ct)
        t4 = time.time()

    return 'Pandas :',t2-t1, 'Cylon :', t4-t3



print('AND :',compare_time('AND'))
print('OR :',compare_time('OR'))















