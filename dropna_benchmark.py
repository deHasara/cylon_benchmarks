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

new_values = [None for i in range(num_rows-1)]
new_values.append(100) #for cylon: column needs atleast one not null otherwise segmentation fault happens

df['data0'] = pd.DataFrame(new_values)
#print(df)
t1 = time.time()
#df1 = df.dropna()
df2 = df.dropna(axis='columns')
#df3 = df.dropna(axis='rows')
#print(df3)
t2 = time.time()



ct['data0'] = Table.from_list(ctx, ['x'], [new_values])
#print(ct)
t3 = time.time()
#axis: 0 for column and 1 for row
ct1 = ct.dropna(axis=0, how='any')
#ct2 = ct.dropna(axis=1, how='any')
#print(ct1)
t4 = time.time()

print('pandas :',t2 - t1, 'cylon :',t4 - t3)

