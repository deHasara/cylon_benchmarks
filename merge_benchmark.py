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

data1 = np.random.randn(num_rows)
df1 = pd.DataFrame({'data{}'.format(i): data
                   for i in range(100)})
df1['key'] = np.random.randint(0, 100, size=num_rows)
ct1 = Table.from_pandas(ctx, df1)
ct1.set_index(range(0, num_rows))


t1 = time.time()
new_df = df.merge(df1, how = 'outer')
#print(new_df)
t2 = time.time()

#print(ct)
t3 = time.time()
new_ct =  Table.merge(ctx, [ct, ct1])
#print(new_ct)
t4 = time.time()

print('pandas :',t2 - t1, 'cylon :',t4 - t3)

