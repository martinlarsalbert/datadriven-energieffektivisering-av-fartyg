import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os



def test_write_read(tmpdir):

    parquet_file = os.path.join(str(tmpdir),'test.parquet')

    df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
    N = 9
    parquet_schema = pa.Table.from_pandas(df=df).schema

    # Open a Parquet file for writing
    with pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy') as parquet_writer:

        for chunk in np.array_split(df, N):
        

            # Write CSV chunk to the parquet file
            table = pa.Table.from_pandas(chunk, schema=parquet_schema)
            parquet_writer.write_table(table)

    ## Reading:
    df2 = pd.read_parquet(parquet_file, engine='pyarrow')

    assert (df2 == df).all().all()