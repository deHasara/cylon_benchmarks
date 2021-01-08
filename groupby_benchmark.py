import time
import numpy as np
import pandas as pd
import pyarrow as pa
from pycylon import Table
from pycylon import CylonContext
from pycylon.data.aggregates import AggregationOp


ctx: CylonContext = CylonContext(config=None, distributed=False)
num_rows = 10 #10_000_000
duplicate_factor = 0.9
gen_record_size = int(num_rows * duplicate_factor)
data = np.random.randint(gen_record_size, size = num_rows)
df = pd.DataFrame({'data{}'.format(i): data
                   for i in range(100)})
#print(df)

ct = Table.from_pandas(ctx, df)
ct.set_index(range(0, num_rows))

col_name = df.columns[0]
t1 = time.time()
new_df = df.groupby([col_name]).sum()
#print(new_df)
t2 = time.time()

#print(ct)
aggeragate_cols = [i+1 for i in range(100-1)]
#print(aggeragate_cols)
aggeragate_ops = [AggregationOp.SUM for i in range(100-1)]
#print(len(aggeragate_ops))
t3 = time.time()
new_ct = ct.groupby(0, aggeragate_cols, aggeragate_ops).sort(0)
#print(new_ct)
t4 = time.time()

print('pandas :',t2 - t1, 'cylon :',t4 - t3)

