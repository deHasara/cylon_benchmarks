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

map_func = lambda x: x + x

t1 = time.time()
new_df = df.applymap(map_func)
t2 = time.time()

#print(ct)
t3 = time.time()
new_ct = ct.applymap(map_func)
#print(new_ct)
t4 = time.time()

print('pandas :',t2 - t1, 'cylon :',t4 - t3)

