from pandas import DataFrame
import numpy as np
from pycylon import CylonContext
from pycylon import Table
import time

npr = np.random.random((10_000_000,100))
df = DataFrame(npr)

ctx: CylonContext = CylonContext(config=None, distributed=False)
ct: Table = Table.from_pandas(ctx, df)

t1 = time.time()
neg_df = -df
t2 = time.time()

t3 = time.time()
neg_ct: Table = -ct
t4 = time.time()

print('pandas :',t2 - t1, 'cylon :',t4 - t3)
