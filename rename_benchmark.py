import pyarrow as pa
import numpy as np
import pandas as pd
import pycylon as cn
from pycylon import Table
from pycylon import CylonContext


import time
#rename column headers

ctx: CylonContext = CylonContext(config=None, distributed=False)
num_rows = 100 #10_000_000
data = np.random.randn(num_rows)

df = pd.DataFrame({'data{}'.format(i): data
                   for i in range(100)})

df['key'] = np.random.randint(0, 100, size=num_rows)
rb = pa.record_batch(df)
t = pa.Table.from_pandas(df)

ct = Table.from_pandas(ctx, df)
ct.set_index(range(0, num_rows))

ct_columns_renamed=[]
for i in range(100):
    ct_columns_renamed.append('changed{}'.format(i))


dict = {}
for i in range(100):
    dict['data{}'.format(i)] = ct_columns_renamed[i]

t1 = time.time()
new_df = df.rename(columns = dict, inplace=True)
t2 = time.time()
#print(df)

t3 = time.time()
new_ct = ct.rename(ct_columns_renamed)
t4 = time.time()

print('pandas :',t2 - t1, 'cylon :',t4 - t3)
