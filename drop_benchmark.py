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

drop_col_list = ['data{}'.format(i) for i in range(90)]

t1 = time.time()
new_df = df.drop(columns=drop_col_list)
#print(new_df)
t2 = time.time()

#print(ct)
t3 = time.time()
new_ct = ct.drop(drop_col_list)
#print(new_ct)
t4 = time.time()

print('pandas :',t2 - t1, 'cylon :',t4 - t3)

