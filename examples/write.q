n:1000000

t:([]
     bool:n?0b;
     guid:n?0Ng;
     byte:n?0x00;
     short:n?100h;
     int:n?100i;
     long:n?100;
     real:n?100e;
     float:n?100f;
     char:n?.Q.a;
     syms:n?`4;
     strings:string n?`8;
     timestamp:n?.z.p;
     month:n?`month$.z.d;
     date:n?.z.d;
     datetime:n?.z.z;
     timespan:n?.z.n;
     minute:n?`minute$.z.t;
     second:n?`second$.z.t;
     time:n?.z.t)
     
//Write the above file to parquet as 2 row groups
\ts .pq.writeMulti[500000#t;`t.parquet]
//1826 55051024
\ts .pq.writeMulti[-500000#t;`t.parquet]
//1826 55051024

//Close the writer to avoid corrupt footer
.pq.closeWriter[]

.pq.load`t.parquet

//Read the first half of the parquet file
\ts t1:.pq.readMulti[]
//729 138196912

//Open and read the first half of the file only
\ts t2:.pq.read`t.parquet
//671 138196896
t1~t2

.pq.next[]
\ts t1,:.pq.readMulti[]
//796 198489936

//Close the file when done reading
.pq.close[]

//Write down a table with a single row group only
\ts .pq.write[t;`t1.parquet]
//3875 928

//Open and read the first row group
\ts t2:.pq.read`t1.parquet
//1288 276392864
t1~t2

//Columns types that can't be extracted directly
//from the parquet file due to lacking logical types
t1:update guid:sv/:[0x00;guid] from t1
t1:update byte:byte[;0] from t1
t1:update char:char[;0] from t1
t1:update syms:`$syms from t1
t1:update month:`month$month from t1
t1:update datetime:`datetime$datetime from t1
t1:update timespan:`timespan$timespan from t1
t1:update minute:`minute$minute from t1
t1:update second:`second$second from t1
t~t1

//import numpy as np
//import pandas as pd
//import pyarrow as pa
//import pyarrow.parquet as pq
//table = pq.read_table('t.parquet')
//table.to_pandas()