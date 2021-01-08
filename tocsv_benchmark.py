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

csv_write_options = CSVWriteOptions().with_delimiter(',')

t1 = time.time()
df.to_csv('/tmp/temp_record.csv')
t2 = time.time()

t3 = time.time()
ct.to_csv('/tmp/temp_record_ct.csv', csv_write_options)
t4 = time.time()

print('pandas :',t2 - t1, 'cylon :',t4 - t3)