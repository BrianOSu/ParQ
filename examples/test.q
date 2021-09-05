//------------------------------------------------------
// Set the environment variables and table
//------------------------------------------------------

//Set total table length
n:1000000
//Set how many rows will go into each rowgroup
n1:"i"$n%2

t:([]
     bool:n?0b;
     guid:n?0Ng;
     byte:n?0x00;
     short:n?0Wh;
     int:n?0Wi;
     long:n?0W;
     real:n?100e;
     float:n?100f;
     char:n?.Q.a;
     syms:n?`4;
     strings:string n?`8;
     timestamp:n?.z.p;
     month:n?`month$.z.d;
     date:n?.z.d;
     datetime:n?.z.z;
     // Pyarrow doesn't like nanoseconds that don't finish with 000 (non-zero nanos)
     // ".z.n" works. "n?.z.n" generates non-zero nanos which are non-compatible with pyarrow
     // https://github.com/apache/arrow/blob/edd88d7d222598550e4812c94194cbf973b20456/cpp/src/arrow/python/datetime.cc#L195
     timespan:n#.z.n; 
     minute:n?`minute$.z.t;
     second:n?0Wv;
     time:n?.z.t)


//------------------------------------------------------
// Test writing Multiple row groups to a parquet file
//------------------------------------------------------

//Write the above file to parquet as 2 row groups
\ts .pq.write.multi[n1#t;`t.parquet]
//622 55051072   <-1mm
//4556 880804672 <-10mm
\ts .pq.write.multi[neg[n1]#t; `t.parquet]
//639 55051136
//4762 880804736

//Close the writer to avoid corrupt footer
.pq.write.close[]


//------------------------------------------------------
// Test reading in Multiple row group parquet file
//------------------------------------------------------

.pq.read.load`t.parquet

//Read the first row group of the parquet file
\ts t1:.pq.read.multi[]
//359 90721072
//3973 1259524912

//Open and read the first row group of the file only
\ts t2:.pq.read.first`t.parquet
//343 90721184
//3808 1259525024
t1~t2

//Read specific columns from second row group
\ts t3:.pq.read.group[`t.parquet; 1; `int`bool]
//4 2622032
//60 41943632
t3~neg[n1]#select int,bool from t

.pq.read.next[]
\ts t1,:.pq.read.multi[]
//399 151538448
//4131 2232603408
//Close the file when done reading
.pq.read.close[]


//------------------------------------------------------
// Test writing/reading a single row group parquet file
//------------------------------------------------------

//Write down a table with a single row group only
\ts .pq.write.single[t;`t1.parquet]
//1028 928
//9604 928

//Open and read the row group
\ts t2:.pq.read.first`t1.parquet
//661 181441440
//6697 2519049120
t1~t2


//------------------------------------------------------
// Show Parquet types that need to be converted back
// to their corresponding KDB types
//------------------------------------------------------

//Columns types that can't be extracted directly
//from the parquet file due to lacking logical types
t1:update char:char[;0] from t1
t1:update month:`month$month from t1
t1:update datetime:`datetime$datetime from t1
t1:update minute:`minute$minute from t1
t1:update second:`second$second from t1
t~t1


//------------------------------------------------------
// Python example
//------------------------------------------------------

//import numpy as np
//import pandas as pd
//import pyarrow as pa
//import pyarrow.parquet as pq
//table = pq.read_table('t.parquet')
//table.schema
//table.to_pandas()

//import pandas as pd
//import pyarrow as pa
//import pyarrow.parquet as pq
//dataframe = pd.DataFrame([[1, 1, 3], [1, 1, 3], [1, 1, 3],], columns=['boop', 'id', 'beep'])
//table_from_pandas = pa.Table.from_pandas(dataframe)
//pq.write_table(table_from_pandas, 'test.parquet')
