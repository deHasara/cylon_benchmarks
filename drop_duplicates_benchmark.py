import time
import numpy as np
import pandas as pd
import pyarrow as pa
from pycylon import Table
from pycylon import CylonContext


ctx: CylonContext = CylonContext(config=None, distributed=False)

num_rows = 10 #10_000_000
duplicate_factor = 0.9
gen_record_size = int(num_rows * duplicate_factor)
data = np.random.randint(gen_record_size, size = num_rows)
print(data)
df = pd.DataFrame({'data{}'.format(i): data
                   for i in range(100)})
#print(df)

ct = Table.from_pandas(ctx, df)
ct.set_index(range(0, num_rows))

column_list = ['data{}'.format(i) for i in range(100)]
#print(column_list)

t1 = time.time()
new_df = df.drop_duplicates()
#print(new_df)
t2 = time.time()
print(len(new_df))

#print(ct)
t3 = time.time()
new_ct = ct.unique(columns=column_list, keep='first')
#print(new_ct)
t4 = time.time()
print(new_ct.row_count)

print('pandas :',t2 - t1, 'cylon :',t4 - t3)

