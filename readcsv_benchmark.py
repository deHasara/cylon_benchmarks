import pyarrow as pa
import numpy as np
import pandas as pd
import pycylon as cn
from pycylon import CylonContext
from pycylon import Table
from pycylon.index import RangeIndex
from pycylon.io import CSVReadOptions
from pycylon.io import CSVWriteOptions
from pycylon.io import read_csv

import time

ctx: CylonContext = CylonContext(config=None, distributed=False)

table_path = 'temp_record.csv'
csv_read_options = CSVReadOptions().use_threads(True).block_size(1 << 30)

t1 = time.time()
df = pd.read_csv(table_path)
t2 = time.time()

t3 = time.time()
ct = read_csv(ctx, table_path, csv_read_options)
t4 = time.time()

print('pandas :',t2 - t1, 'cylon :',t4 - t3)
#print(tb3.row_count)
#print(tb3.column_count)
#assert tb3.row_count == 10000 and tb3.column_count == 102