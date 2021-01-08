import numpy as np
import pandas as pd
import pycylon as cn
from pycylon import CylonContext
from pycylon import Table
from pycylon.index import RangeIndex
from pycylon.io import CSVReadOptions
from pycylon.io import CSVWriteOptions

import time

ctx: CylonContext = CylonContext(config=None, distributed=False)
num_rows = 100 #10_000_000
data = np.random.randn(num_rows)

df = pd.DataFrame({'data{}'.format(i): data
                   for i in range(100)})

df['key'] = np.random.randint(0, 100, size=num_rows)

ct = Table.from_pandas(ctx, df)
ct.set_index(range(0, num_rows))

#print(df)

t1 = time.time()
new_df = df.div(10)
#print(df.div(10))
t2 = time.time()


t3 = time.time()
new_ct = ct/10
#print(ct)
t4 = time.time()

print('pandas :',t2 - t1, 'cylon :',t4 - t3)